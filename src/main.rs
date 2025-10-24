#![allow(unused_imports)]
use std::collections::{vec_deque, HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::Notify;

const CRLF_TERMINATOR_LEN: usize = 2;

const SUPPORT_COMMANDS: [&str; 10] = ["ping", "echo", "set", "get", "rpush", "lpush", "lrange", "llen", "lpop", "blpop"];

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
    background_task: Notify,
}

impl DB {
    pub fn new() -> Self {
        let db = DB {
            entries: HashMap::new(),
            pub_sub: HashMap::new(),
            background_task: Notify::new(),
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

    pub fn blpop(&mut self, keys: VecDeque<String>, timeout: f32) -> (String, String) {
        let mut result: (String, String) = (String::new(), String::new());

        for key in keys {
            let entry = self.entries.get_mut(&key);
            if let Some(entry) = entry {
                if let EntryValue::List(ref mut list) = entry.value {
                    if let Some(element) = list.pop_front() {
                        result = (key, element);
                    }
                }
            }
        }

        result
    }

    pub fn remove(&mut self, key: &String) -> Option<Entry> {
        self.entries.remove(key)
    }
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let app_state = AppState::new();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let map = Arc::clone(&app_state.db);
        tokio::spawn(async move {
            handle_response(stream, map).await;
        });
    }
}

async fn handle_response(mut stream: TcpStream, map: Arc<Mutex<DB>>) {
    loop {
        let mut buf = [0; 512];
        let size = stream.read(&mut buf).await.unwrap();
        if size <= 0 {
            break;
        }
        let command = command_parser(buf);
        match command {
            Ok(command) => {
                let response: String;
                {
                    let mut entries = map.lock().unwrap();
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
                            entries.set(&command.key, entry);
                            "+OK\r\n".to_string()
                        },

                        "get" => match entries.get(&command.key) {
                            Some(entry) => match &entry.value {
                                EntryValue::String(s) => format!("${}\r\n{}\r\n", s.len(), s),
                                _ => "$-1\r\n".to_string(),
                            },
                            None => "$-1\r\n".to_string(),
                        },

                        "rpush" => {
                            if let EntryValue::List(list) = command.value.clone() {
                                let len = entries.rpush(&command.key, list);
                                format!(":{}\r\n", len)
                            } else {
                                "-ERR wrong type\r\n".to_string()
                            }
                        },

                        "lpush" => {
                            if let EntryValue::List(list) = command.value.clone() {
                                let len = entries.lpush(&command.key, list);
                                format!(":{}\r\n", len)
                            } else {
                                "-ERR wrong type\r\n".to_string()
                            }
                        }

                        "lrange" => {
                            match command.option {
                                Some(CommandOption::LRange(mut option)) => {
                                    match entries.get(&command.key) {
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
                            match entries.get(&command.key) {
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
                                match entries.lpop(&command.key, count) {
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
                                                let (key, value) = entries.blpop(keys, blpop_opt.timeout);
                                                format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value)
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

                        _ => "-ERR unknown command\r\n".to_string(),
                    };
                }

                let _  = stream.write(response.as_bytes()).await;
            },
            Err(_e) => {
                let _ = stream.write(String::from("-ERR unknown command\r\n").as_bytes()).await;
            }
        }
    }
}

fn command_parser(buf: [u8; 512]) -> Result<Command, CommandParserErr>  {
    let number_of_elements = asc2_to_decimal(buf[1]);

    // First byte: '*' 
    // Second byte: number of elements
    let mut index = 2 + CRLF_TERMINATOR_LEN;
    let mut command = Command {
        name: String::new(),
        key: String::new(),
        value: EntryValue::String(String::new()),
        option: None,
    };

    for i in 0..number_of_elements {
        // character $
        index += 1;
        let len = read_length(&buf, &mut index).unwrap();
        index += CRLF_TERMINATOR_LEN + 1;
        let value = str::from_utf8(&buf[index..index + len]).unwrap().to_lowercase();

        if i == 0 && !SUPPORT_COMMANDS.contains(&value.as_str()) {
            return Err(CommandParserErr::UnknowCommand);
        }

        if i == 0 {
            command.name = value;
            if command.name == "rpush" || command.name == "lpush" || command.name == "blpop" {
                command.value = EntryValue::List(VecDeque::new());
            }
        } else {
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
                    if i == 1 {
                        command.key = value.clone();
                    }
                    
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
                    let timeout = match value.parse::<f32>() {
                        Ok(v) => v,
                        Err(_err) => -1.0
                    };
                    command.option = Some(CommandOption::BLPop(BLPopOption { timeout }));
                }
            }
        }

        index += len + CRLF_TERMINATOR_LEN;
    }

    Ok(command)
}

fn asc2_to_decimal(byte: u8) -> u8 {
    byte - b'0'
}

fn read_length(buf: &[u8], index: &mut usize) -> Result<usize, &'static str> {
    if *index >= buf.len() {
        return Err("invalid index");
    }
    let mut len_buf = Vec::new();
    loop {
        len_buf.push(buf[*index]);
        if buf[*index + 1] == b'\r' {
            break;
        }
        *index += 1;
    }
    let s = String::from_utf8(len_buf).unwrap();
    let len = s.parse::<usize>().unwrap();

    Ok(len)
}

#[derive(Debug)]
enum CommandParserErr {
    UnknowCommand,
    InvalidOption,
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
    timeout: f32,
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