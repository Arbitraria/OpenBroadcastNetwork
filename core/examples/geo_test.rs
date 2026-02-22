use OpenBroadcastNetwork_core::overlay::topology::{GeoLocation, Region};

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

    // Test distance calculations between major cities
    println!("\nDistance calculations:");
    let nyc = GeoLocation::new(40.7128, -74.0060, "US".to_string());
    let london = GeoLocation::new(51.5074, -0.1278, "GB".to_string());
    let tokyo = GeoLocation::new(35.6762, 139.6503, "JP".to_string());
    let sao_paulo = GeoLocation::new(-23.5505, -46.6333, "BR".to_string());

    println!("NYC to London: {:.0} km", nyc.distance_to(&london));
    println!("NYC to Tokyo: {:.0} km", nyc.distance_to(&tokyo));
    println!("London to Tokyo: {:.0} km", london.distance_to(&tokyo));
    println!("NYC to Sao Paulo: {:.0} km", nyc.distance_to(&sao_paulo));

    // Test same region checks
    println!("\nSame region checks:");
    let la = GeoLocation::new(34.0522, -118.2437, "US".to_string());
    let berlin = GeoLocation::new(52.5200, 13.4050, "DE".to_string());

    println!("NYC and LA (both US): {}", nyc.is_same_region(&la));
    println!(
        "London and Berlin (both EU): {}",
        london.is_same_region(&berlin)
    );
    println!("NYC and Tokyo (different): {}", nyc.is_same_region(&tokyo));
}
