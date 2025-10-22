#![allow(unused_imports)]
use std::collections::{vec_deque, HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use tokio::sync::Notify;

const CRLF_TERMINATOR_LEN: usize = 2;

const SUPPORT_COMMANDS: [&str; 7] = ["ping", "echo", "set", "get", "rpush", "lpush", "lrange"];

const SUPPORT_OPTION: [&str; 8] = ["ex", "px", "exat", "pxat", "nx", "xx", "keepttl", "get"];

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

    pub fn remove(&mut self, key: &String) -> Option<Entry> {
        self.entries.remove(key)
    }
}

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let app_state = AppState::new();
    let mut handles = vec![];

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let map = Arc::clone(&app_state.db);
                let handle = thread::spawn(move || {
                    handle_response(stream, map);
                });

                handles.push(handle);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_response(mut stream: TcpStream, map: Arc<Mutex<DB>>) {
    loop {
        let mut buf = [0; 512];
        let size = stream.read(&mut buf).unwrap();
        if size <= 0 {
            break;
        }
        let command = command_parser(buf);
        match command {
            Ok(command) => {
                let mut entries = map.lock().unwrap();
                let response = match command.name.as_str() {
                    "ping" => "+PONG\r\n".to_string(),

                    "echo" => format!("${}\r\n{}\r\n", command.key.len(), command.key),

                    "set" => {
                        let entry = Entry { 
                            value: command.value.clone(), 
                            expires_at: command.expires_at 
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
                        match command.lrange_option {
                            Some(mut option) => {
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
                            }
                            None => "-ERR missing parameter\r\n".to_string(),
                        }
                    }

                    _ => "-ERR unknown command\r\n".to_string(),
                };

                let _  = stream.write_all(response.as_bytes());
            },
            Err(_e) => {
                let _ = stream.write_all(String::from("-ERR unknown command\r\n").as_bytes());
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
        expires_at: None,
        lrange_option: None,
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
            if command.name == "rpush" || command.name == "lpush" {
                command.value = EntryValue::List(VecDeque::new());
            }
        } else if i == 1 {
            command.key = value;
        } else {
            if command.name == "get" || command.name == "set" {
                if i == 2 {
                    let entry_value = EntryValue::String(value.clone());
                    command.value = entry_value;
                }
                
                if i == 3 {
                    let opt = command.option.get_or_insert_with(|| CommandOption { key: String::new(), value: String::new() });
                    if !SUPPORT_OPTION.contains(&value.as_str()) {
                        return Err(CommandParserErr::InvalidOption)
                    }
                    opt.key = value.clone();
                }

                if i == 4 {
                    let opt = command.option.get_or_insert_with(|| CommandOption { key: String::new(), value: String::new() });
                    let value_i = match value.parse::<u64>() {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(CommandParserErr::InvalidOption);
                        }
                    };
                    if opt.get_key() == "ex" {
                        command.expires_at = Some(SystemTime::now() + Duration::from_secs(value_i));
                    }
                    if opt.get_key() == "px" {
                        command.expires_at = Some(SystemTime::now() + Duration::from_millis(value_i));
                    }
                    opt.value = value.clone();
                }
            }

            if command.name == "rpush" || command.name == "lpush" {
                if let EntryValue::List(list) = &mut command.value {
                    list.push_back(value.clone());
                }
            }

            if command.name == "lrange" {
                if i == 2 {
                    let opt = command.lrange_option.get_or_insert_with(|| LRangeOption { start: 0, stop: 0 });
                    opt.start = match value.parse::<i32>() {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(CommandParserErr::InvalidOption);
                        }
                    };
                }

                if i == 3 {
                    let opt = command.lrange_option.get_or_insert_with(|| LRangeOption { start: 0, stop: 0 });
                    opt.stop = match value.parse::<i32>() {
                        Ok(v) => v,
                        Err(e) => {
                            return Err(CommandParserErr::InvalidOption);
                        }
                    };
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

#[derive(Debug, Clone)]
struct Command {
    name: String,
    key: String,
    value: EntryValue,
    option: Option<CommandOption>,
    expires_at: Option<SystemTime>,
    lrange_option: Option<LRangeOption>,
}

#[derive(Debug, Clone)]
struct CommandOption {
    key: String,
    value: String,
}

#[derive(Debug, Clone)]
struct LRangeOption {
    start: i32,
    stop: i32,
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

impl CommandOption {
    pub fn get_key(&self) -> String {
        self.key.clone()
    }
}