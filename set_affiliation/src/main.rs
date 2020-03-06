use std::error::Error;

use ldap3::{LdapConnSettings, LdapConn, Scope, SearchEntry};
use native_tls::TlsConnector;
use clap::{Arg, App};
use users::get_current_username;

fn main() {
    let matches = App::new("Set Affiliation")
                          .arg(Arg::with_name("username")
                               .short("u")
                               .long("username")
                               .value_name("USERNAME")
                               .help("Username to test")
                               .takes_value(true))
                          .arg(Arg::with_name("server")
                               .short("s")
                               .long("server")
                               .value_name("SERVER")
                               .help("LDAP server to connect to")
                               .takes_value(true))
                          .get_matches();

    // Gets a value for config if supplied by user, or defaults to "default.conf"
    let mut username = matches.value_of("username").unwrap_or("");
    let curuser;
    if username.len() < 1 {
        curuser = get_current_username().unwrap();
        username = curuser.to_str().unwrap();
    }
    let server = matches.value_of("server").unwrap_or("ldap-1.icecube.wisc.edu");

    let affiliation = match username {
        "condor" => String::new(),
        "ganglia" => String::new(),
        "root" => String::new(),
        "ice3simusr" => String::from("simprod"),
        user => {
            match do_search(user, server) {
                Ok(x) => match x {
                    Some(val) => val,
                    None => String::from("error"),
                },
                Err(_e) => String::from("error"),
            }
        }
    };
    println!("Affiliation = \"{}\"", affiliation);
}

fn do_search(username: &str, server: &str) -> Result<Option<String>, Box<dyn Error>> {
    let mut builder = TlsConnector::builder();
    builder.danger_accept_invalid_certs(true);
    builder.danger_accept_invalid_hostnames(true);
    let settings = LdapConnSettings::new().set_connector(
        builder.build().unwrap()
    ).set_no_tls_verify(true);
    let server2 = String::from("ldaps://") + server;
    let ldap = LdapConn::with_settings(settings, &server2)?;
    let mut search_field = String::from("(&(uid=");
    search_field += username;
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