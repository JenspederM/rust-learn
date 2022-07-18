use std::io;

// C to F: F = C*(9/5) + 32
// F to C: C = (F-32)*(5/9)

fn convert_temperature(temperature: f64) -> f64 {
    let mut input: String = String::new();

    println!("What are you converting? Celcius (c) or Fahrenheit (f)");

    loop {
        io::stdin().read_line(&mut input).expect("Invalid Input.");

        let input = input.trim().to_lowercase();

        if input == "c" || input == "celcius" {
            let result = (&temperature) * (9.0 / 5.0) + 32.0;
            println!("{temperature} Cº == {result} F");
            return result;
        } else if input == "f" || input == "fahrenheit" {
            let result = (&temperature - 32.0) * (5.0 / 9.0);
            println!("{temperature} F == {result} Cº");
            return result;
        } else {
            println!("Wrong input: {input}Valid options are: celcius (c) or fahrenheit (f)")
        }
    }
}

fn get_temperature() -> f64 {
    println!("Enter temperature that you wish to convert: ");
    loop {
        let mut temperature = String::new();

        io::stdin()
            .read_line(&mut temperature)
            .expect("Something went wrong with stdin.");

        match temperature.trim().parse() {
            Ok(num) => return num,
            Err(_) => {
                println!("Invalid temperature. Try again...");
                continue;
            }
        };
    }
}

fn main() {
    println!("Welcome to the temperature converter");
    println!();
    println!("I Hope you enjoy it!");

    loop {
        println!();
        let mut retry: String = String::new();

        let temperature = get_temperature();
        convert_temperature(temperature);

        println!("Do you want to convert another temperature? yes (y) or no (n)");

        io::stdin()
            .read_line(&mut retry)
            .expect("Something went wrong with stdin.");

        let retry = retry.trim().to_ascii_lowercase();

        if retry == "yes" || retry == "y" {
            continue;
        } else if retry == "no" || retry == "n" {
            break;
        } else {
            println!("Invalid input: {retry}. Valid options are yes (y) or no (n)");
        }
    }

    println!();
    println!("Bye bye")
}
