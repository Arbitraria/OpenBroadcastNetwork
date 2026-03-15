//! Geographic utilities for topology management
//!
//! This module provides geographic awareness for the overlay network,
//! allowing for location-based optimizations.

use crate::overlay::peer;
use std::net::IpAddr;
use std::str::FromStr;

/// GeoIP lookup provider
///
/// In production, this would use a real GeoIP database.
/// For testing, it uses a deterministic mapping based on IP octets.
#[derive(Debug, Clone)]
pub struct GeoIP;

impl GeoIP {
    /// Look up geographic information for an IP address.
    ///
    /// Uses the last octet of the IP address to deterministically select a region:
    /// - 0: NA/US (North America)
    /// - 1: EU/DE (Europe)
    /// - 2: AS/JP (Asia)
    /// - 3: SA/BR (South America)
    pub fn lookup(&self, ip: IpAddr) -> Option<String> {
        match ip {
            IpAddr::V4(ipv4) => {
                let last_octet = ipv4.octets()[3];
                let country = match last_octet % 4 {
                    0 => "US",
                    1 => "DE",
                    2 => "JP",
                    3 => "BR",
                    _ => unreachable!(),
                };
                Some(country.to_string())
            }
            IpAddr::V6(ipv6) => {
                // Use last byte of IPv6 address
                let segments = ipv6.segments();
                let last_byte = (segments[7] & 0xFF) as u8;
                let country = match last_byte % 4 {
                    0 => "US",
                    1 => "DE",
                    2 => "JP",
                    3 => "BR",
                    _ => unreachable!(),
                };
                Some(country.to_string())
            }
        }
    }

    /// Look up and return a full GeoLocation for an IP address.
    pub fn lookup_location(&self, ip: IpAddr) -> Option<GeoLocation> {
        let country = self.lookup(ip)?;

        // Return representative coordinates for each country
        let (lat, lon) = match country.as_str() {
            "US" => (37.0902, -95.7129),  // Center of US
            "DE" => (51.1657, 10.4515),   // Center of Germany
            "JP" => (36.2048, 138.2529),  // Center of Japan
            "BR" => (-14.2350, -51.9253), // Center of Brazil
            _ => (0.0, 0.0),
        };

        Some(GeoLocation::new(lat, lon, country))
    }
}

/// Geographic region codes (simplified)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    /// North America (US, CA, MX)
    NorthAmerica,
    /// South America (BR, AR, CL, etc.)
    SouthAmerica,
    /// Europe (GB, DE, FR, etc.)
    Europe,
    /// Asia (CN, JP, KR, etc.)
    Asia,
    /// Africa (ZA, NG, EG, etc.)
    Africa,
    /// Oceania (AU, NZ)
    Oceania,
    /// Region could not be determined
    Unknown,
}

impl FromStr for Region {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "na" | "north_america" => Ok(Region::NorthAmerica),
            "sa" | "south_america" => Ok(Region::SouthAmerica),
            "eu" | "europe" => Ok(Region::Europe),
            "as" | "asia" => Ok(Region::Asia),
            "af" | "africa" => Ok(Region::Africa),
            "oc" | "oceania" => Ok(Region::Oceania),
            _ => Ok(Region::Unknown),
        }
    }
}

impl std::fmt::Display for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Region::NorthAmerica => "north_america",
            Region::SouthAmerica => "south_america",
            Region::Europe => "europe",
            Region::Asia => "asia",
            Region::Africa => "africa",
            Region::Oceania => "oceania",
            Region::Unknown => "unknown",
        };
        write!(f, "{}", s)
    }
}

/// Geographic location information
#[derive(Debug, Clone)]
pub struct GeoLocation {
    /// Latitude
    pub latitude: f64,
    /// Longitude
    pub longitude: f64,
    /// Country code
    pub country: String,
    /// Region
    pub region: Region,
    /// City
    pub city: Option<String>,
    /// ISP/ASN information
    pub asn: Option<String>,
}

impl Default for GeoLocation {
    fn default() -> Self {
        Self {
            latitude: 0.0,
            longitude: 0.0,
            country: String::from("XX"),
            region: Region::Unknown,
            city: None,
            asn: None,
        }
    }
}

impl GeoLocation {
    /// Create a new location from coordinates and country
    pub fn new(latitude: f64, longitude: f64, country: String) -> Self {
        let region = Self::country_to_region(&country);

        Self {
            latitude,
            longitude,
            country,
            region,
            city: None,
            asn: None,
        }
    }

    /// Calculate distance to another location (using Haversine formula)
    pub fn distance_to(&self, other: &GeoLocation) -> f64 {
        // Earth radius in kilometers
        const EARTH_RADIUS: f64 = 6371.0;

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

    /// Check if this location is in the same region as another
    pub fn is_same_region(&self, other: &GeoLocation) -> bool {
        self.region == other.region
    }

    /// Get region from country code
    pub fn country_to_region(country: &str) -> Region {
        // This is a simplified mapping and not comprehensive
        match country {
            "US" | "CA" | "MX" => Region::NorthAmerica,
            "BR" | "AR" | "CL" | "CO" | "PE" | "VE" => Region::SouthAmerica,
            "GB" | "DE" | "FR" | "IT" | "ES" | "NL" | "SE" | "NO" | "FI" | "PL" => Region::Europe,
            "CN" | "JP" | "KR" | "IN" | "SG" | "TH" | "MY" | "ID" => Region::Asia,
            "ZA" | "NG" | "EG" | "KE" | "MA" => Region::Africa,
            "AU" | "NZ" => Region::Oceania,
            _ => Region::Unknown,
        }
    }

    /// Create a location for a peer based on its IP address
    pub fn from_peer_info(peer: &peer::PeerInfo) -> Self {
        // If the peer already has region info, use that
        if let Some(region_str) = peer.metadata.get("region") {
            if let Ok(region) = Region::from_str(region_str) {
                return Self {
                    latitude: peer
                        .metadata
                        .get("latitude")
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    longitude: peer
                        .metadata
                        .get("longitude")
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    country: peer
                        .metadata
                        .get("country")
                        .cloned()
                        .unwrap_or_else(|| "XX".to_string()),
                    region,
                    city: peer.metadata.get("city").cloned(),
                    asn: peer.metadata.get("asn").cloned(),
                };
            }
        }

        // Try to determine location from IP addresses
        let geo_ip = GeoIP;
        for addr in &peer.addresses {
            if let Some(ip) = Self::extract_ip_from_addr(addr) {
                if let Some(location) = geo_ip.lookup_location(ip) {
                    return location;
                }
            }
        }

        // Use default if no info available
        Self::default()
    }

    /// Extract IP address from a peer address string
    fn extract_ip_from_addr(addr: &str) -> Option<IpAddr> {
        addr.split(':')
            .next()
            .and_then(|ip_str| ip_str.parse::<IpAddr>().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_geoip_lookup_deterministic() {
        let geo = GeoIP;

        // Test that different IPs get different regions based on last octet
        let ip_us = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 0)); // 0 % 4 = 0 -> US
        let ip_de = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)); // 1 % 4 = 1 -> DE
        let ip_jp = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)); // 2 % 4 = 2 -> JP
        let ip_br = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 3)); // 3 % 4 = 3 -> BR
        let ip_us2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4)); // 4 % 4 = 0 -> US

        assert_eq!(geo.lookup(ip_us), Some("US".to_string()));
        assert_eq!(geo.lookup(ip_de), Some("DE".to_string()));
        assert_eq!(geo.lookup(ip_jp), Some("JP".to_string()));
        assert_eq!(geo.lookup(ip_br), Some("BR".to_string()));
        assert_eq!(geo.lookup(ip_us2), Some("US".to_string()));
    }

    #[test]
    fn test_geoip_lookup_location() {
        let geo = GeoIP;

        let ip_us = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 0));
        let loc = geo.lookup_location(ip_us).unwrap();

        assert_eq!(loc.country, "US");
        assert_eq!(loc.region, Region::NorthAmerica);
        assert!(loc.latitude > 30.0 && loc.latitude < 50.0); // Roughly US latitude
    }

    #[test]
    fn test_distance_calculation() {
        // New York to London is approximately 5,570 km
        let ny = GeoLocation::new(40.7128, -74.0060, "US".to_string());
        let london = GeoLocation::new(51.5074, -0.1278, "GB".to_string());

        let distance = ny.distance_to(&london);
        assert!(distance > 5000.0 && distance < 6000.0);
    }

    #[test]
    fn test_same_region() {
        let us = GeoLocation::new(40.0, -100.0, "US".to_string());
        let ca = GeoLocation::new(45.0, -75.0, "CA".to_string());
        let de = GeoLocation::new(51.0, 10.0, "DE".to_string());

        assert!(us.is_same_region(&ca)); // Both North America
        assert!(!us.is_same_region(&de)); // US vs Europe
    }
}
