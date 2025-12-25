use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[cfg(test)]
mod unit_tests;

#[derive(Serialize, Deserialize, Debug)]
struct Task {
    id: u32,
    data: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct TaskResult {
    id: u32,
    word_count: HashMap<String, u32>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Registration {
    ip: String,
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut port = 9000;
    let listener = loop {
        if let Ok(l) = TcpListener::bind(("127.0.0.1", port)).await {
            break l;
        }
        port += 1;
        if port > 9100 {
            panic!("Не удалось найти свободный порт!");
        }
    };

    println!("Worker слушает на 127.0.0.1:{}", port);

    register_with_planner(port).await?;

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("Новое соединение от {}", addr);

        tokio::spawn(async move {
            let mut buffer = vec![0u8; 1024 * 1024];
            match socket.read(&mut buffer).await {
                Ok(n) if n == 0 => return,
                Ok(n) => {
                    let task: Task =
                        serde_json::from_slice(&buffer[..n]).expect("Ошибка разбора JSON");
                    println!("Получено задание");

                    let word_count = count_words(&task.data);

                    let result = TaskResult {
                        id: task.id,
                        word_count,
                    };

                    let result_json = serde_json::to_vec(&result).unwrap();
                    socket.write_all(&result_json).await.unwrap();
                    println!("Результат отправлен Planner");
                }
                Err(e) => eprintln!("Ошибка чтения: {}", e),
            }
        });
    }
}

fn count_words(text: &str) -> HashMap<String, u32> {
    let mut map = HashMap::new();
    for word in text.split_whitespace() {
        let word = word.to_lowercase();
        *map.entry(word).or_insert(0) += 1;
    }
    map
}

async fn register_with_planner(port: u16) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8000").await?;
    let registration = Registration {
        ip: "127.0.0.1".to_string(),
        port,
    };
    let msg = serde_json::to_vec(&registration)?;
    stream.write_all(&msg).await?;
    println!("Worker зарегистрирован у Planner-а: {:?}", registration);
    Ok(())
}
