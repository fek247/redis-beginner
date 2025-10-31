#![allow(unused_imports)]
use std::collections::{vec_deque, HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::{Mutex, Notify};
use tokio::time::Timeout;

const CRLF_TERMINATOR_LEN: usize = 2;

const SUPPORT_COMMANDS: [&str; 12] = ["ping", "echo", "set", "get", "rpush", "lpush", "lrange", "llen", "lpop", "blpop", "type", "xadd"];

const SET_SUPPORT_OPTION: [&str; 8] = ["ex", "px", "exat", "pxat", "nx", "xx", "keepttl", "get"];

struct AppState {
    db: Arc<Mutex<DB>>,
}

impl AppState {
    pub fn new() -> Self {
        let state = AppState {
            db: Arc::new(Mutex::new(DB::new())),
        };
        state
    }
}

struct DB {
    entries: HashMap<String, Entry>,
    pub_sub: HashMap<String, String>,
    waiting_task: HashMap<String, Arc<tokio::sync::Notify>>,
}

impl DB {
    pub fn new() -> Self {
        let db = DB {
            entries: HashMap::new(),
            waiting_task: HashMap::new(),
            pub_sub: HashMap::new(),
        };
        db
    }

    pub fn set(&mut self, key: &String, entry: Entry) {
        self.entries.insert(key.to_string(), entry);
    }

    pub fn get(&mut self, key: &String) -> Option<&Entry> {
        let expires_at = self.entries.get(key).and_then(|v| v.expires_at);
        if let Some(exp) = expires_at {
            if exp < SystemTime::now() {
                self.remove(key);
                return None;
            }
        };

        self.entries.get(key)
    }

    pub fn rpush(&mut self, key: &String, values: VecDeque<String>) -> usize {
        self.entries
            .entry(key.to_string())
            .and_modify(|entry| {
                if let EntryValue::List(ref mut list) = entry.value {
                    list.extend(values.clone());
                } else {
                    entry.value = EntryValue::List(values.clone());
                }
            })
            .or_insert_with(|| Entry {
                value: EntryValue::List(values),
                expires_at: None,
            });

        if let Some(entry) = self.entries.get(key) {
            if let EntryValue::List(list) = &entry.value {
                return list.len();
            }
        }

        0
    }

    pub fn lpush(&mut self, key: &String, values: VecDeque<String>) -> usize {
        self.entries
            .entry(key.to_string())
            .and_modify(|entry| {
                if let EntryValue::List(ref mut list) = entry.value {
                    for s in values.iter() {
                        list.push_front(s.to_string());
                    }
                } else {
                    println!("Err");
                }
            })
            .or_insert_with(|| Entry {
                value: EntryValue::List(values.into_iter().rev().collect()),
                expires_at: None,
            });

        if let Some(entry) = self.entries.get(key) {
            if let EntryValue::List(list) = &entry.value {
                return list.len();
            }
        }

        0
    }

    pub fn lpop(&mut self, key: &String, mut count: usize) -> Result<Vec<String>, OperationErr> {
        let entry = self.entries.get_mut(key);
        match entry {
            Some(entry) => {
                if let EntryValue::List(ref mut list ) = entry.value {
                    let len = list.len();
                    if count > len {
                        count = len;
                    }

                    let mut result = Vec::<String>::new();
                    for _i in 0..count {
                        // Unwrap here because already validate index
                        result.push(list.pop_front().unwrap());
                    }

                    Ok(result)
                } else {
                    return Err(OperationErr::WrongType);
                }
            },
            None => {
                return Ok(vec![]);
            }
        }
    }

    pub async fn blpop(db_mutex: Arc<Mutex<DB>>, keys: VecDeque<String>, timeout: f64) -> (String, String) {
        let duration = tokio::time::Duration::from_secs_f64(timeout);

        loop {
            let mut db_guard = db_mutex.lock().await;

            for key in &keys {
                if let Some(entry) = db_guard.entries.get_mut(key) {
                    if let EntryValue::List(list) = &mut entry.value {
                        if let Some(value) = list.pop_front() {
                            return (key.clone(), value);
                        }
                    }
                }
            }

            let mut notify_handle: Option<Arc<Notify>> = None;
            let mut key_to_wait = String::new();

            if let Some(key) = keys.front() {
                key_to_wait = key.clone();

                let notify = db_guard.waiting_task.entry(key.clone()).or_insert_with(|| Arc::new(Notify::new())).clone();

                notify_handle = Some(notify);
            }

            drop(db_guard);

            if let Some(notify) = notify_handle {
                if timeout == 0.0 {
                    notify.notified().await;
                    continue;
                } else {
                    match tokio::time::timeout(duration, notify.notified()).await {
                        Ok(_) => {
                            continue;
                        },
                        Err(_) => {
                            return ("".to_string(), "".to_string());
                        }
                    }
                }
            } else {
                return ("".to_string(), "".to_string());
            }
        }
    }

    pub fn remove(&mut self, key: &String) -> Option<Entry> {
        self.entries.remove(key)
    }

    pub fn notify_waiting_tasks(&mut self, key: &str) {
        if let Some(notify) = self.waiting_task.get(key) {
            notify.notify_one();

            self.waiting_task.remove(key);
        }
    }

    pub fn xadd(&mut self, key: &String, stream_id: String, stream_entries: Vec<XAddPair>) {
        let new_entry = StreamEntryValue {
            id: stream_id,
            pairs: stream_entries,
        };

        self.entries
            .entry(key.to_string())
            .and_modify(|entry| {
                if let EntryValue::Stream(ref mut map) = entry.value {
                    map.push(new_entry.clone());
                } else {
                    println!("Err");
                }
            })
            .or_insert_with(|| Entry {
                value: EntryValue::Stream(vec![new_entry]),
                expires_at: None,
            });
    }
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let app_state = AppState::new();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let db = Arc::clone(&app_state.db);
        tokio::spawn(async move {
            handle_response(stream, db).await;
        });
    }
}

async fn handle_response(mut stream: TcpStream, app_state: Arc<Mutex<DB>>) {
    loop {
        let mut buf = [0; 512];
        let size = stream.read(&mut buf).await.unwrap();
        if size <= 0 {
            break;
        }
        let command = command_parser(buf, &app_state).await;
        match command {
            Ok(command) => {
                let response: String;
                {
                    let mut db_guard = app_state.lock().await;
                    response = match command.name.as_str() {
                        "ping" => "+PONG\r\n".to_string(),

                        "echo" => format!("${}\r\n{}\r\n", command.key.len(), command.key),

                        "set" => {
                            let expires_at = match command.option {
                                Some(opt) => {
                                    if let CommandOption::Set(set_opt) = opt {
                                        Some(set_opt.expires_at)
                                    } else {
                                        None
                                    }
                                },
                                None => None,
                            };
                            let entry = Entry {
                                value: command.value.clone(), 
                                expires_at,
                            };
                            db_guard.set(&command.key, entry);
                            "+OK\r\n".to_string()
                        },

                        "get" => match db_guard.get(&command.key) {
                            Some(entry) => match &entry.value {
                                EntryValue::String(s) => format!("${}\r\n{}\r\n", s.len(), s),
                                _ => "$-1\r\n".to_string(),
                            },
                            None => "$-1\r\n".to_string(),
                        },

                        "rpush" => {
                            if let EntryValue::List(list) = command.value.clone() {
                                let len = db_guard.rpush(&command.key, list);
                                db_guard.notify_waiting_tasks(&command.key);
                                format!(":{}\r\n", len)
                            } else {
                                "-ERR wrong type\r\n".to_string()
                            }
                        },

                        "lpush" => {
                            if let EntryValue::List(list) = command.value.clone() {
                                let len = db_guard.lpush(&command.key, list);
                                format!(":{}\r\n", len)
                            } else {
                                "-ERR wrong type\r\n".to_string()
                            }
                        }

                        "lrange" => {
                            match command.option {
                                Some(CommandOption::LRange(mut option)) => {
                                    match db_guard.get(&command.key) {
                                        Some(entry) => {
                                            if let EntryValue::List(list) = &entry.value {
                                                let len = list.len() as i32;

                                                if option.start >= len {
                                                    "*0\r\n".to_string()
                                                } else {
                                                    option.start = if option.start < 0 { len + option.start } else { option.start };
                                                    option.stop  = if option.stop  < 0 { len + option.stop  } else { option.stop  };

                                                    if option.stop >= len { option.stop = len - 1; }
                                                    if option.start < 0 { option.start = 0; }
                                                    if option.stop < 0 { option.stop = 0; }

                                                    if option.start > option.stop {
                                                        "*0\r\n".to_string()
                                                    } else {
                                                        let slice = list.range(option.start as usize..=option.stop as usize);
                                                        let mut result = format!("*{}\r\n", slice.len());
                                                        for s in slice {
                                                            result.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
                                                        }
                                                        result
                                                    }
                                                }
                                            } else {
                                                "*0\r\n".to_string()
                                            }
                                        }
                                        None => "*0\r\n".to_string(),
                                    }
                                },
                                Some(_) => "-ERR wrong command option type\r\n".to_string(),
                                None => "-ERR missing parameter\r\n".to_string(),
                            }
                        }

                        "llen" => {
                            match db_guard.get(&command.key) {
                                Some(entry) => {
                                    if let EntryValue::List(list) = &entry.value {
                                        format!(":{}\r\n", list.len())
                                    } else {
                                        ":0\r\n".to_string()
                                    }
                                },
                                None => ":0\r\n".to_string(),
                            }
                        }

                        "lpop" => {
                            let (count, wrong_type)  = match command.option {
                                Some(CommandOption::LPop(option)) => (option.count, false),
                                Some(_) => (1, true),
                                None => (1, false)
                            };

                            if wrong_type {
                                "-ERR wrong command option type\r\n".to_string()
                            } else {
                                match db_guard.lpop(&command.key, count) {
                                    Ok(list) => {
                                        match list.len() {
                                            0 => {
                                                "$-1\r\n".to_string()
                                            },
                                            1 => {
                                                format!("${}\r\n{}\r\n", list[0].len(), list[0])
                                            },
                                            _ => {
                                                let mut result = format!("*{}\r\n", list.len());
                                                for s in list {
                                                    result.push_str(&format!("${}\r\n{}\r\n", s.len(), s));
                                                }
                                                result
                                            }
                                        }
                                    },
                                    Err(e) => "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string(),
                                }
                            }
                        }

                        "blpop" => {
                            match command.option {
                                Some(opt) => {
                                    if let CommandOption::BLPop(blpop_opt) = opt {
                                        if blpop_opt.timeout != -1.0 {
                                            if let EntryValue::List(keys) = command.value {
                                                drop(db_guard);
                                                let app_state_clone = app_state.clone();
                                                let (key, value) = DB::blpop(app_state_clone, keys, blpop_opt.timeout).await;
                                                if key.is_empty() && value.is_empty() {
                                                    "*-1\r\n".to_string()
                                                } else {
                                                    format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value)
                                                }   
                                            } else {
                                                "-ERR WRONGTYPE Option\r\n".to_string()
                                            }
                                        } else {
                                            "-ERR missing timeout param\r\n".to_string()
                                        }
                                    } else {
                                        "-ERR WRONGTYPE Option\r\n".to_string()
                                    }
                                },
                                None => "-ERR missing param\r\n".to_string(),
                            }
                        }

                        "type" => {
                            match db_guard.get(&command.key) {
                                Some(entry) => match &entry.value {
                                    EntryValue::String(_) => "+string\r\n".to_string(),
                                    EntryValue::List(_) => "+list\r\n".to_string(),
                                    EntryValue::Set(_) => "+set\r\n".to_string(),
                                    EntryValue::Stream(_) => "+stream\r\n".to_string(),
                                    _ => "+none\r\n".to_string(),
                                },
                                None => "+none\r\n".to_string(),
                            }
                        }

                        "xadd" => {
                            match command.option {
                                Some(opt) => {
                                    if let CommandOption::XAdd(pairs) = opt {
                                        if let EntryValue::String(stream_id) = command.value {
                                            db_guard.xadd(&command.key, stream_id.clone(), pairs);
                                            format!("${}\r\n{}\r\n", stream_id.len(), stream_id)
                                        } else {
                                            "-ERR WRONGTYPE Option\r\n".to_string()
                                        }
                                    } else {
                                        "-ERR WRONGTYPE Option\r\n".to_string()
                                    }
                                },
                                None => "-ERR wrong number of arguments for 'XADD' command".to_string()
                            }
                        }

                        _ => "-ERR unknown command\r\n".to_string(),
                    };
                }

                let _  = stream.write(response.as_bytes()).await;
            },
            Err(e) => {
                let err_response = match e {
                    CommandParserErr::XAddKeyNotEqual0 => {
                        "-ERR The ID specified in XADD must be greater than 0-0\r\n".to_string()
                    }
                    CommandParserErr::XAddKeyNotValid => {
                        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".to_string()
                    }
                    _ => {
                        "-ERR unknown command\r\n".to_string()
                    }
                };

                let _ = stream.write(err_response.as_bytes()).await;
            }
        }
    }
}

async fn command_parser(buf: [u8; 512], app_state: &Arc<Mutex<DB>>) -> Result<Command, CommandParserErr>  {
    // First byte: '*'
    let mut index = 0;
    // Second byte: number of elements
    let (number_of_elements, consumed_header) = match read_length(&buf[index..], b'*') {
        Ok(v) => v,
        Err(e) => return Err(e), 
    };

    index += consumed_header;

    if number_of_elements == 0 {
        return Err(CommandParserErr::Invalid);
    }

    let mut command = Command {
        name: String::new(),
        key: String::new(),
        value: EntryValue::String(String::new()),
        option: None,
    };

    for i in 0..number_of_elements {
        if index >= buf.len() {
            return Err(CommandParserErr::Invalid);
        }

        let (bulk_len, consumed_header) = match read_length(&buf[index..], b'$') {
            Ok(v) => v,
            Err(e) => return Err(e),
        };

        index += consumed_header;

        if index + bulk_len + CRLF_TERMINATOR_LEN > buf.len() {
            return Err(CommandParserErr::Invalid); 
        }

        let value = str::from_utf8(&buf[index..index + bulk_len]).unwrap().to_lowercase();

        if i == 0 && !SUPPORT_COMMANDS.contains(&value.as_str()) {
            return Err(CommandParserErr::UnknowCommand);
        }

        if i == 0 {
            command.name = value;
            if command.name == "rpush" || command.name == "lpush" || command.name == "blpop" {
                command.value = EntryValue::List(VecDeque::new());
            }
        } else {
            if command.name == "echo" && i == 1 {
                command.key = value.clone();
            }

            if command.name == "get" || command.name == "set" {
                if i == 1 {
                    command.key = value.clone();
                }

                if i == 2 {
                    let entry_value = EntryValue::String(value.clone());
                    command.value = entry_value;
                }
                
                if i == 3 {
                    let opt = command.option.get_or_insert_with(|| CommandOption::Set(SetOption {
                        key: String::new(),
                        value: String::new(),
                        expires_at: SystemTime::now(),
                    }));
                    if !SET_SUPPORT_OPTION.contains(&value.as_str()) {
                        return Err(CommandParserErr::InvalidOption)
                    }

                    if let CommandOption::Set(opt) = opt {
                        opt.set_key(value.clone());
                    }
                }

                if i == 4 {
                    let opt = command.option.get_or_insert_with(|| CommandOption::Set(SetOption {
                        key: String::new(),
                        value: String::new(),
                        expires_at: SystemTime::now(),
                    }));
                    let value_i = match value.parse::<u64>() {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(CommandParserErr::InvalidOption);
                        }
                    };
                    if let CommandOption::Set(set_opt) = opt {
                        if set_opt.get_key() == "ex" {
                            set_opt.expires_at = SystemTime::now() + Duration::from_secs(value_i);
                        }
                        if set_opt.get_key() == "px" {
                            set_opt.expires_at = SystemTime::now() + Duration::from_millis(value_i);
                        }
                        set_opt.set_value(value.clone());
                    }

                }
            }

            if command.name == "rpush" || command.name == "lpush" {
                if i == 1 {
                    command.key = value.clone();
                } else {
                    if let EntryValue::List(list) = &mut command.value {
                        list.push_back(value.clone());
                    }
                }
            }

            if command.name == "llen" && i == 1 {
                command.key = value.clone();
            }

            if command.name == "lrange" {
                if i == 1 {
                    command.key = value.clone();
                }

                if i == 2 {
                    let opt = command.option.get_or_insert_with(|| CommandOption::LRange(LRangeOption { start: 0, stop: 0 }));
                    if let CommandOption::LRange(lrange_opt) = opt {
                        lrange_opt.start = match value.parse::<i32>() {
                            Ok(v) => v,
                            Err(e) => {
                                return Err(CommandParserErr::InvalidOption);
                            }
                        };
                    }
                }

                if i == 3 {                    
                    let opt = command.option.get_or_insert_with(|| CommandOption::LRange(LRangeOption { start: 0, stop: 0 }));
                    if let CommandOption::LRange(lrange_opt) = opt {
                        lrange_opt.stop = match value.parse::<i32>() {
                            Ok(v) => v,
                            Err(e) => {
                                return Err(CommandParserErr::InvalidOption);
                            }
                        };
                    }
                }
            }

            if command.name == "lpop" {
                if i == 1 {
                    command.key = value.clone();
                }

                if i == 2 {
                    let count = match value.parse::<usize>() {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(CommandParserErr::InvalidOption);
                        }
                    };
                    command.option = Some(CommandOption::LPop(LPopOption { count: count }));
                }
            }

            if command.name == "blpop" {
                if i > 0 && i < number_of_elements - 1 {
                    if let EntryValue::List(list) = &mut command.value {
                        list.push_back(value.clone());
                    }
                }

                if i == number_of_elements - 1 {
                    let timeout = match value.parse::<f64>() {
                        Ok(v) => v,
                        Err(_err) => -1.0
                    };
                    command.option = Some(CommandOption::BLPop(BLPopOption { timeout }));
                }
            }

            if command.name == "type" && i == 1 {
                command.key = value.clone();
            }
                
            if command.name == "xadd" {
                if i == 1 {
                    command.key = value.clone();
                }

                if i == 2 {
                    if String::eq(&value, "0-0") {
                        return Err(CommandParserErr::XAddKeyNotEqual0);
                    }

                    let db_guard = app_state.lock().await;
                    let entry = db_guard.entries.get(&command.key);
                    let last_stream_entry = match entry {
                        Some(entry) => {
                            if let EntryValue::Stream(ref stream_entrys) = entry.value {
                                match stream_entrys.last() {
                                    Some(stream_entry) => {
                                        stream_entry.id.clone()
                                    },
                                    None => "0-0".to_string()
                                }
                            } else {
                                "-1".to_string()
                            }
                        },
                        None => "0-0".to_string()
                    };
        
                    drop(db_guard);

                    let is_valid = check_valid_stream_id(&value, &last_stream_entry);

                    if !is_valid {
                        return Err(CommandParserErr::XAddKeyNotValid);
                    }

                    let formated_value = format_stream_id(&value, &last_stream_entry);

                    command.value = EntryValue::String(formated_value);
                }

                if i > 2 && i % 2 == 1 {
                    let opt = command.option.get_or_insert_with(|| CommandOption::XAdd(vec![]));
                    if let CommandOption::XAdd(list) = opt {
                        let xadd_opt = XAddPair { key: value.clone(), value: String::new() };
                        list.push(xadd_opt);
                    }
                }

                if i > 2 && i % 2 == 0 {
                    let opt = command.option.get_or_insert_with(|| CommandOption::XAdd(vec![]));
                    if let CommandOption::XAdd(list) = opt {
                        if let Some(last) = list.last_mut() {
                            last.value = value.clone();
                        }
                    }
                }
            }
        }

        index += bulk_len + CRLF_TERMINATOR_LEN;
    }

    Ok(command)
}

fn read_length(buf: &[u8], prefix: u8) -> Result<(usize, usize), CommandParserErr> {
    if buf.is_empty() || buf[0] != prefix {
        return Err(CommandParserErr::Invalid);
    }

    if let Some(crlf_index) = buf[1..].windows(2).position(|window| window == [b'\r', b'\n']) {
        let end_of_number = 1 + crlf_index;

        let number_bytes = &buf[1..end_of_number];

        if let Ok(number_str) = str::from_utf8(number_bytes) {
            match number_str.parse::<usize>() {
                Ok(size) => {
                    let consumed_bytes = end_of_number + 2; 

                    Ok((size, consumed_bytes))
                },
                Err(_) => Err(CommandParserErr::Invalid),
            }
        } else {
            Err(CommandParserErr::Invalid)
        }
    } else {
        Err(CommandParserErr::Invalid)
    }
}

fn check_valid_stream_id(value: &str, last_stream_entry: &str) -> bool {
    let parts: Vec<&str> = value.split('-').collect();
    let parts_last_stream: Vec<&str> = last_stream_entry.split('-').collect();
    if parts.len() == 1 && parts[0] == "*" {
        return true;
    }

    if parts.len() != 2 || parts_last_stream.len() != 2 {
        return false;
    }

    if parts[0] < parts_last_stream[0] {
        return false;
    }

    if parts[1] == "*" {
        return true;
    }

    if parts[0] == parts_last_stream[0] && parts[1] <= parts_last_stream[1] {
        return false;
    }

    true
}

fn format_stream_id(value: &str, last_stream_entry: &str) -> String {
    let parts: Vec<&str> = value.split('-').collect();
    let parts_last_stream: Vec<&str> = last_stream_entry.split('-').collect();

    if parts.len() == 1 && parts[0] == "*" {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("System time before UNIX EPOCH!").as_millis().to_string();
        let seq = if String::eq(&timestamp, parts_last_stream[0]) {
            parts_last_stream[1].parse::<u64>().unwrap() + 1
        } else {
            0
        };

        return format!("{}-{}", timestamp, seq);
    } else if parts[1] == "*" {
        let seq = if parts[0] > parts_last_stream[0] {
            0
        } else {
            parts_last_stream[1].parse::<u64>().unwrap() + 1
        };

        return format!("{}-{}", parts[0], seq);
    }

    value.to_string()
}

#[derive(Debug)]
enum CommandParserErr {
    UnknowCommand,
    InvalidOption,
    Invalid,
    XAddKeyNotValid,
    XAddKeyNotEqual0,
}

enum OperationErr {
    WrongType,
}

#[derive(Debug, Clone)]
struct Command {
    name: String,
    key: String,
    value: EntryValue,
    option: Option<CommandOption>,
}

#[derive(Debug, Clone)]
enum CommandOption {
    Set(SetOption),
    LRange(LRangeOption),
    LPop(LPopOption),
    BLPop(BLPopOption),
    XAdd(Vec<XAddPair>),
}
#[derive(Debug, Clone)]
struct SetOption {
    key: String,
    value: String,
    expires_at: SystemTime,
}

#[derive(Debug, Clone)]
struct LRangeOption {
    start: i32,
    stop: i32,
}

#[derive(Debug, Clone)]
struct LPopOption {
    count: usize,
}

#[derive(Debug, Clone)]
struct BLPopOption {
    timeout: f64,
}

#[derive(Debug, Clone)]
struct XAddPair {
    key: String,
    value: String,
}

#[derive(Debug, Clone)]
struct StreamEntryValue {
    id: String,
    pairs: Vec<XAddPair>,
}

#[derive(Debug, Clone)]
struct Entry {
    value: EntryValue,
    expires_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
enum EntryValue {
    String(String),
    List(VecDeque<String>),
    Map(HashMap<String, String>),
    Set(Vec<String>),
    Stream(Vec<StreamEntryValue>),
}

impl SetOption {
    pub fn get_key(&self) -> String {
        self.key.clone()
    }

    pub fn set_key(&mut self, key: String) {
        self.key = key;
    }

    pub fn set_value(&mut self, value: String) {
        self.value = value;
    }
}