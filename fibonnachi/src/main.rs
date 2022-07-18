use cached::proc_macro::cached;
use std::io;

#[cached]
fn fibonacci(n: i64) -> i64 {
    if n <= 1 {
        return n;
    }
    return fibonacci(n - 1) + fibonacci(n - 2);
}

fn main() {
    let mut input = String::new();

    println!("Enter a number to get it's Fibonacci value");

    io::stdin().read_line(&mut input).expect("Error");

    let input = match input.trim().parse() {
        Ok(num) => num,
        Err(_) => -1,
    };

    let result = fibonacci(input);

    println!("Fibonacci of {} is {}", input, result)
}
