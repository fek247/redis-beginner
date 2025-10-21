#![allow(unused_imports)]
use std::collections::{HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use tokio::sync::Notify;

const CRLF_TERMINATOR_LEN: usize = 2;

const SUPPORT_COMMANDS: [&str; 4] = ["ping", "echo", "set", "get"];

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

    pub fn get(&mut self, key: &String) -> String {
        let value = self.entries.get(key);
        match value {
            Some(val) => {
                match val.expires_at {
                    Some(expires_at) => {
                        if (expires_at < SystemTime::now()) {
                            return String::new();
                        }

                        return val.value.to_string();
                    },
                    None => val.value.to_string(),
                }
            },
            None => String::new(),
        }
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
            Ok(mut command) => {
                let mut entries = map.lock().unwrap();
                if command.name == "set" {
                    let entry = Entry { value: command.value.clone(), expires_at: command.expires_at };
                    entries.set(&command.key, entry);
                }
                if command.name == "get" {
                    command.value = entries.get(&command.key);
                }
                let _ = stream.write_all(command.response().as_bytes());
            },
            Err(e) => {
                println!("error: {:?}", e);
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
        value: String::new(),
        option: None,
        expires_at: None,
    };

    for i in 0..number_of_elements {
        // character $
        index += 1;
        let len = read_length(&buf, &mut index).unwrap();
        index += CRLF_TERMINATOR_LEN + 1;
        let value = str::from_utf8(&buf[index..index + len]).unwrap().to_lowercase();

        if SUPPORT_COMMANDS.contains(&value.as_str()) {
            command.name = value
        } else {
            if command.name.is_empty() {
                return Err(CommandParserErr::UnknowCommand);
            }

            if i == 1 {
                command.key = value.clone();
            }

            if i == 2 {
                command.value = value.clone();
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
    InvalidFormat,
    UnknowCommand,
    InvalidOption,
}

struct Command {
    name: String,
    key: String,
    value: String,
    option: Option<CommandOption>,
    expires_at: Option<SystemTime>,
}

struct CommandOption {
    key: String,
    value: String,
}

struct Entry {
    value: String,
    expires_at: Option<SystemTime>,
}

impl Command {
    pub fn response(&self) -> String {
        match self.name.as_str() {
            "ping" => String::from("+PONG\r\n"),
            "echo" => format!("${}\r\n{}\r\n", self.key.len(), self.key),
            "get"  => {
                if self.value.is_empty() {
                    return String::from("$-1\r\n");
                }

                format!("${}\r\n{}\r\n", self.value.len(), self.value)
            },
            "set"  => String::from("+OK\r\n"),
            _      => String::from("-ERR unknown command\r\n"),
        }
    }
}

impl CommandOption {
    pub fn get_key(&self) -> String {
        self.key.clone()
    }

    pub fn get_value(&self) -> String {
        self.value.clone()
    }
}