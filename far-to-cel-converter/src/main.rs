use std::io;

// C to F: F = C*(9/5) + 32
// F to C: C = (F-32)*(5/9)

const CONVERSION_TYPES: [&str; 2] = ["Celcius -> Fahrenheit", "Fahrenheit -> Celcius"];

fn get_conversion_type() -> &'static str {
    println!("Type 'C' to convert Fahrenheit to Celcius or 'F' to convert Celcius to Fahrenheit.");

    loop {
        let mut conversion_type: String = String::new();

        io::stdin()
            .read_line(&mut conversion_type)
            .expect("Invalid Input.");

        let conversion_type = conversion_type.trim().to_string();

        if conversion_type.to_uppercase() == "C" {
            return CONVERSION_TYPES[0];
        } else if conversion_type.to_uppercase() == "F" {
            return CONVERSION_TYPES[1];
        } else {
            println!("Invalid value... Try again.")
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

fn convert(conversion_type: &str, temperature: f64) -> f64 {
    if conversion_type == CONVERSION_TYPES[0] {
        return (temperature) * (9.0 / 5.0) + 32.0;
    } else {
        return (temperature - 32.0) * (5.0 / 9.0);
    }
}

fn main() {
    let conversion_type = get_conversion_type();
    let temperature = get_temperature();
    let result: f64 = convert(conversion_type, temperature);
    println!("Converting {temperature} {conversion_type} is equal to {result}");
}
