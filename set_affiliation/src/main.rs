extern crate ldap3;

use std::error::Error;

use ldap3::{LdapConnSettings, LdapConn, Scope, SearchEntry};
use native_tls::TlsConnector;

fn main() {
    match do_search(String::from("dschultz")) {
        Ok(x) => match x {
            Some(val) => println!("{}", val),
            None => println!("none"),
        },
        Err(_e) => println!("none"),
    }
}

fn do_search(username: String) -> Result<Option<String>, Box<dyn Error>> {
    let mut builder = TlsConnector::builder();
    builder.danger_accept_invalid_certs(true);
    builder.danger_accept_invalid_hostnames(true);
    let settings = LdapConnSettings::new().set_connector(
        builder.build().unwrap()
    ).set_no_tls_verify(true);
    let ldap = LdapConn::with_settings(settings, "ldaps://ldap-1.icecube.wisc.edu")?;
    let mut search_field = String::from("(&(uid=");
    search_field += &username;
    search_field += "))";
    let (rs, _res) = ldap.search(
        "ou=People,dc=icecube,dc=wisc,dc=edu",
        Scope::Subtree,
        &search_field,
        vec!["o"]
    )?.success()?;
    for entry in rs {
        let se = SearchEntry::construct(entry);
        //println!("{:?}", se);
        let inst = &se.attrs["o"][0];
        let (rs, _res) = ldap.search(
            inst,
            Scope::Subtree,
            "(cn=*)",
            vec!["cn"]
        )?.success()?;
        for entry in rs {
            let se = SearchEntry::construct(entry);
            //println!("{:?}", se);
            return Ok(Some(se.attrs["cn"][0].clone()));
        }
        break;
    }
    Ok(None)
}