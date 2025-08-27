#![allow(unused_imports)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

const SUPPORT_COMMANDS: [&str; 2] = ["ping", "echo"];

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
        println!("message: {:?}", buf);
        if size <= 0 {
            break;
        }
        let _ = command_parser(buf, &mut &stream);
        let _ = stream.write_all("+PONG\r\n".as_bytes());
    }
}

fn command_parser(buf: [u8; 512], stream: &mut &TcpStream) -> Result<(), CommandParserErr>  {
    let number_of_elements = asc2_to_decimal(buf[1]);
    let crlf_terminator_len = 2;
    // if cfg!(windows) {
    //     crlf_terminator_len = 2;
    // }
    println!("len down line: {crlf_terminator_len}");
    // First byte: '*' 
    // Second byte: number of elements
    let mut index = 2 + crlf_terminator_len;
    for _ in 0..number_of_elements {
        // character $
        index += 1;
        let len = asc2_to_decimal(buf[index]) as usize;
        println!("command length: {len}");
        index += 1;
        let command = str::from_utf8(&buf[index..index+len]).unwrap().to_lowercase();
        // println!("{}", command);
        if SUPPORT_COMMANDS.contains(&command.as_str()) {
            return Err(CommandParserErr::InvalidFormat);
        } else {
            println!("Command: {command}");
            if command.eq("ping") {
                let _ = stream.write_all("+PONG\r\n".as_bytes());
            } else if command.eq("echo") {
                println!("hey");
            }
        }
        index += len;
        break;
    }

    Ok(())
}

fn asc2_to_decimal(byte: u8) -> u8 {
    byte - b'0'
}

#[derive(Debug)]
enum CommandParserErr {
    InvalidFormat,
    UnknownCommand
}
