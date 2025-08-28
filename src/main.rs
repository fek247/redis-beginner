#![allow(unused_imports)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

const CRLF_TERMINATOR_LEN: usize = 2;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(|| {
                    handle_response(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_response(mut stream: TcpStream) {
    loop {
        let mut buf = [0; 512];
        let size = stream.read(&mut buf).unwrap();
        if size <= 0 {
            break;
        }
        let _ = command_parser(buf, &mut &stream);
    }
}

fn command_parser(buf: [u8; 512], stream: &mut &TcpStream) -> Result<(), CommandParserErr>  {
    let number_of_elements = asc2_to_decimal(buf[1]);
    // First byte: '*' 
    // Second byte: number of elements
    let mut index = 2 + CRLF_TERMINATOR_LEN;
    let mut commands: Vec<String> = vec![];
    for _ in 0..number_of_elements {
        // character $
        index += 1;
        let len = read_length(&buf, &mut index).unwrap();
        index += CRLF_TERMINATOR_LEN + 1;
        let command = str::from_utf8(&buf[index..index + len]).unwrap().to_lowercase();
        commands.push(command);
        let last_command = commands.last().unwrap();
        if commands.last().unwrap().eq("ping") || commands.last().unwrap().eq("echo") {
            if last_command == "ping" {
                let _ = stream.write_all("+PONG\r\n".as_bytes());
            }
        } else {
            let previous_command = commands.get(commands.len().saturating_sub(2)).unwrap();
            if commands.len() >= 2 && previous_command == "echo" {
                let response = format!("${}\r\n{}\r\n", last_command.len(), last_command);
                let _ = stream.write_all(response.as_bytes());
            } else {
                return Err(CommandParserErr::InvalidFormat);
            }
        }

        index += len + CRLF_TERMINATOR_LEN;
    }

    Ok(())
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
}