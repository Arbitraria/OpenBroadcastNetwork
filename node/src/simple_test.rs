use std::net::IpAddr;
use std::str::FromStr;
use std::collections::HashMap;

// Simple representation of a geographic region
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Region {
    NorthAmerica,
    SouthAmerica,
    Europe,
    Asia,
    Africa,
    Oceania,
    Unknown,
}

impl ToString for Region {
    fn to_string(&self) -> String {
        match self {
            Region::NorthAmerica => "north_america".to_string(),
            Region::SouthAmerica => "south_america".to_string(),
            Region::Europe => "europe".to_string(),
            Region::Asia => "asia".to_string(),
            Region::Africa => "africa".to_string(),
            Region::Oceania => "oceania".to_string(),
            Region::Unknown => "unknown".to_string(),
        }
    }
}

// Simplified geo-location
#[derive(Debug, Clone)]
struct GeoLocation {
    latitude: f64,
    longitude: f64,
    country: String,
    region: Region,
}

impl GeoLocation {
    fn new(latitude: f64, longitude: f64, country: String) -> Self {
        let region = match country.as_str() {
            "US" | "CA" | "MX" => Region::NorthAmerica,
            "BR" | "AR" | "CL" => Region::SouthAmerica,
            "GB" | "DE" | "FR" | "IT" => Region::Europe,
            "CN" | "JP" | "KR" | "IN" => Region::Asia,
            "ZA" | "NG" | "EG" => Region::Africa,
            "AU" | "NZ" => Region::Oceania,
            _ => Region::Unknown,
        };
        
        Self { latitude, longitude, country, region }
    }
    
    // Calculate distance using Haversine formula
    fn distance_to(&self, other: &GeoLocation) -> f64 {
        const EARTH_RADIUS: f64 = 6371.0; // km
        
        let lat1 = self.latitude.to_radians();
        let lon1 = self.longitude.to_radians();
        let lat2 = other.latitude.to_radians();
        let lon2 = other.longitude.to_radians();
        
        let dlat = lat2 - lat1;
        let dlon = lon2 - lon1;
        
        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        
        EARTH_RADIUS * c
    }
    
    fn is_same_region(&self, other: &GeoLocation) -> bool {
        self.region == other.region
    }
}

// Simplified peer representation
#[derive(Debug, Clone)]
struct PeerInfo {
    id: String,
    location: Option<GeoLocation>,
}

impl PeerInfo {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            location: None,
        }
    }
    
    fn with_location(mut self, lat: f64, lon: f64, country: &str) -> Self {
        self.location = Some(GeoLocation::new(lat, lon, country.to_string()));
        self
    }
}

fn main() {
    println!("=== Testing Geo-Aware Functionality ===\n");
    
    // Create test locations
    let sf = GeoLocation::new(37.7749, -122.4194, "US".to_string());
    let london = GeoLocation::new(51.5074, -0.1278, "GB".to_string());
    let tokyo = GeoLocation::new(35.6762, 139.6503, "JP".to_string());
    
    println!("Test locations:");
    println!("- San Francisco: {:?}", sf);
    println!("- London: {:?}", london);
    println!("- Tokyo: {:?}", tokyo);
    
    println!("\nDistance calculations:");
    println!("SF to London: {:.2} km", sf.distance_to(&london));
    println!("SF to Tokyo: {:.2} km", sf.distance_to(&tokyo));
    println!("London to Tokyo: {:.2} km", london.distance_to(&tokyo));
    
    println!("\nRegion checks:");
    println!("SF same region as London? {}", sf.is_same_region(&london));
    println!("SF same region as Tokyo? {}", sf.is_same_region(&tokyo));
    
    // Simulate network of peers with locations
    let mut peers = HashMap::new();
    peers.insert("peer1".to_string(), PeerInfo::new("peer1").with_location(37.7749, -122.4194, "US"));
    peers.insert("peer2".to_string(), PeerInfo::new("peer2").with_location(51.5074, -0.1278, "GB"));
    peers.insert("peer3".to_string(), PeerInfo::new("peer3").with_location(35.6762, 139.6503, "JP"));
    peers.insert("peer4".to_string(), PeerInfo::new("peer4").with_location(19.4326, -99.1332, "MX"));
    peers.insert("peer5".to_string(), PeerInfo::new("peer5").with_location(55.7558, 37.6173, "RU"));
    
    println!("\nPeer proximity simulation:");
    
    // Find closest peers to a target (US-West)
    let target = GeoLocation::new(37.7749, -122.4194, "US".to_string());
    
    let mut peer_distances: Vec<(&String, f64)> = peers.iter()
        .filter_map(|(id, peer)| {
            peer.location.as_ref().map(|loc| (id, loc.distance_to(&target)))
        })
        .collect();
    
    peer_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    
    println!("Peers sorted by distance to San Francisco:");
    for (id, distance) in peer_distances {
        if let Some(location) = &peers[id].location {
            println!("- {}: {:.2} km ({:?})", id, distance, location.region);
        }
    }
    
    println!("\n=== Geo-Aware Test Complete ===");
} 