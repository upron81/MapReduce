use reqwest;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// Адрес Planner
const PLANNER_URL: &str = "http://127.0.0.1:8080/upload";

/// Генерация тестового текста: lines строк, words_per_line слов
fn generate_text(lines: usize, words_per_line: usize) -> String {
    let mut text = String::new();
    for i in 0..lines {
        for j in 0..words_per_line {
            text.push_str(&format!("word{}_{} ", i, j));
        }
        text.push('\n');
    }
    text
}

/// Маленький хелпер для имитации join_all без futures
async fn join_all_tokio<T>(handles: Vec<tokio::task::JoinHandle<T>>) -> Vec<T> {
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    results
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn load_test_upload_endpoint() {
    let client = reqwest::Client::new();

    let concurrent_requests = 30; // количество параллельных запросов
    let lines = 1000; // строки в каждом тексте
    let words_per_line = 100; // слова на строку

    let payload = generate_text(lines, words_per_line);

    println!(
        "Запуск нагрузочного теста: {} параллельных запросов, ~{} слов в каждом",
        concurrent_requests,
        lines * words_per_line
    );

    let start = Instant::now();

    let mut handles = Vec::new();

    for _ in 0..concurrent_requests {
        let client = client.clone();
        let payload = payload.clone();

        let handle = tokio::spawn(async move {
            let form = reqwest::multipart::Form::new().text("file", payload);

            let req_start = Instant::now();
            let res = client.post(PLANNER_URL).multipart(form).send().await;

            match res {
                Ok(r) => {
                    let status = r.status();
                    let elapsed = req_start.elapsed();
                    if status.is_success() {
                        println!("OK: {} ms", elapsed.as_millis());
                        true
                    } else {
                        println!("ERR status: {}", status);
                        false
                    }
                }
                Err(e) => {
                    println!("REQUEST ERROR: {}", e);
                    false
                }
            }
        });

        handles.push(handle);

        // небольшая пауза между запросами, чтобы нагрузка была "волнами"
        sleep(Duration::from_millis(5)).await;
    }

    let results = join_all_tokio(handles).await;

    let success_count = results.into_iter().filter(|r| *r).count();
    let elapsed = start.elapsed();

    println!("========== РЕЗУЛЬТАТ ==========");
    println!("Успешных запросов: {}", success_count);
    println!("Всего запросов: {}", concurrent_requests);
    println!(
        "Среднее время одного запроса: {:.2} ms",
        elapsed.as_millis() as f64 / concurrent_requests as f64
    );
    println!("Общее время теста: {:?}\n", elapsed);

    // Проверяем, что хотя бы 80% запросов успешны
    assert!(success_count >= concurrent_requests * 80 / 100);
}
