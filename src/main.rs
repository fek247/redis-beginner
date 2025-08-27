#![allow(unused_imports)]
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                handle_response(_stream)
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
        let _ = stream.write_all("+PONG\r\n".as_bytes());
    }
}
