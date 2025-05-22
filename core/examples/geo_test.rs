use decentralized_stream_core::overlay::topology::{GeoLocation, Region};
use std::net::IpAddr;
use std::str::FromStr;

fn main() {
    println!("Testing geo-aware functionality");
    
    // Create a couple of geo locations
    let loc1 = GeoLocation::new(37.7749, -122.4194, "US".to_string());
    let loc2 = GeoLocation::new(51.5074, -0.1278, "GB".to_string());
    
    println!("Location 1: {:?}", loc1);
    println!("Location 2: {:?}", loc2);
    
    // Calculate distance between them
    let distance = loc1.distance_to(&loc2);
    println!("Distance between locations: {:.2} km", distance);
    
    // Check if they're in the same region
    let same_region = loc1.is_same_region(&loc2);
    println!("Are locations in the same region? {}", same_region);
    
    // Test region detection from country codes
    let countries = ["US", "BR", "DE", "JP", "ZA", "AU"];
    for country in countries {
        let location = GeoLocation::new(0.0, 0.0, country.to_string());
        println!("Country: {}, Region: {:?}", country, location.region);
    }
    
    // Test IP address extraction
    let addr = "192.168.1.1:8080";
    match GeoLocation::extract_ip_from_addr(addr) {
        Some(ip) => println!("Extracted IP from {}: {}", addr, ip),
        None => println!("Could not extract IP from {}", addr),
    }
    
    // Test random location generation (used in our implementation for testing)
    println!("\nGenerating random locations:");
    for _ in 0..3 {
        if let Some(location) = GeoLocation::lookup_ip_location(&IpAddr::from_str("127.0.0.1").unwrap()) {
            println!("Location: {:?}", location);
        }
    }
} 