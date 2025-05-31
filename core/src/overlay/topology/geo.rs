//! Geographic utilities for topology management
//!
//! This module provides geographic awareness for the overlay network,
//! allowing for location-based optimizations.

use std::str::FromStr;
use std::net::IpAddr;
use crate::overlay::peer;

// Placeholder for GeoIP until a working implementation is provided
#[derive(Debug, Clone)]
pub(crate) struct GeoIP;

impl GeoIP {
    pub fn lookup(&self, _ip: IpAddr) -> Option<String> {
        // Default to Unknown region
        Some("XX".to_string())
    }
}

/// Geographic region codes (simplified)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    NorthAmerica,
    SouthAmerica,
    Europe,
    Asia,
    Africa,
    Oceania,
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
    fn country_to_region(country: &str) -> Region {
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
                    latitude: peer.metadata.get("latitude")
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    longitude: peer.metadata.get("longitude")
                        .and_then(|v| v.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    country: peer.metadata.get("country")
                        .cloned()
                        .unwrap_or_else(|| "XX".to_string()),
                    region,
                    city: peer.metadata.get("city").cloned(),
                    asn: peer.metadata.get("asn").cloned(),
                };
            }
        }
        
        // Try to determine location from IP addresses
        for addr in &peer.addresses {
            if let Some(ip) = Self::extract_ip_from_addr(addr) {
                if let Some(location) = Self::lookup_ip_location(&ip) {
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
    
    /// Look up location information for an IP address
    fn lookup_ip_location(_ip: &IpAddr) -> Option<Self> {
        // In a real implementation, this would use a GeoIP database
        // For testing, we'll return a placeholder location
        Some(Self::default())
    }
}
