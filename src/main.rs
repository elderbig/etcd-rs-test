use etcd_rs::*;
use std::time::{Duration, SystemTime};
use futures::{StreamExt,FutureExt};
use etcd_rs::{Client, LeaseGrantRequest, PutRequest, LeaseKeepAliveRequest, LeaseGrantResponse};
use log::{info, error, debug};
use anyhow::Result;


#[tokio::main]
async fn main() -> Result<()> {
    log4rs::init_file("conf/log4rs.yaml", Default::default()).unwrap();
    let mut interval = tokio::time::interval(Duration::from_secs(6));
    let mut is_conn = false;
    while !is_conn {
        let result = etcd_rs::Client::connect(ClientConfig {
            endpoints: vec!["http://127.0.0.1:12379".to_owned(),"http://127.0.0.1:22379".to_owned(),"http://127.0.0.1:32379".to_owned()],
            auth: None,
            tls: None,
        })
        .await;
        let client = match result{
            Ok(c)=>c,
            Err(e)=>{
                is_conn = false;
                error!("{}", e);
                interval.tick().await;
                continue;
            }
        };
        let c = client.clone();
        let mut s = Vec::new();
        for i in 0..=100{
            s.push(format!("{}", i))
        }
        let task = async move { keep_alive_lease(&c).await};
        let result =tokio::task::spawn(task).await?;
        match result{
            Ok(_)=>{
                is_conn = true;
                let c = client.clone();
                tokio::signal::ctrl_c()
                .then(|_| async { c.shutdown().await })
                .await?;
            },
            Err(e)=>{
                is_conn = false;
                client.shutdown().await?;
                error!("{}", e);
                interval.tick().await; 
            }
        }
        info!("Reconnect etcd ...");
        // interval.tick().await;
    }
    // let key = "foo";
    // let value = "bar";

    // // Put a key-value pair
    // let resp = client.kv().put(PutRequest::new(key, value)).await?;

    // println!("Put Response: {:#?}", resp);
    // for k in 1..10000 {
    //     let begin_time = SystemTime::now();
    //     for i in 1..100000 {
    //         client.kv().put(PutRequest::new(format!("{}.{}", k, i), "for循环是有条件的循环, 即循环运行特定的次数。 rust语言中for循环的行为与其他语言略有不同。执行for循环直到条件为真。")).await?;
    //     }
    //     println!("time in second = {:?}, {} times for 100000, total {} keys", SystemTime::now().duration_since(begin_time).unwrap().as_secs(), k, k*100000);
    // }

    // println!("Get Response: {:?}", resp);
    // for k in 1..10000 {
    //     let begin_time = SystemTime::now();
    //     for i in 1..100000 {
    //         client.kv().range(RangeRequest::new(KeyRange::key(format!("{}.{}", k, i)))).await?;
    //     }
    //     println!("time in second = {:?}, {} times for 100000, total {} keys", SystemTime::now().duration_since(begin_time).unwrap().as_secs(), k, k*100000);
    // }

    // println!("Deleting Response: {:?}", resp);
    // for k in 1..100 {
    //     let begin_time = SystemTime::now();
    //     for i in 1..100000 {
    //         client.kv().delete(DeleteRequest::new(KeyRange::key(format!("{}.{}", k, i)))).await?;
    //     }
    //     println!("time in second = {:?}, {} times for 100000, total {} keys", SystemTime::now().duration_since(begin_time).unwrap().as_secs(), k, k*100000);
    // }

    // println!("Deleting");
    // let begin_time = SystemTime::now();
    // for i in 1..100000 {
    //     client.kv().delete(DeleteRequest::new(KeyRange::key(format!("{}", i)))).await?;
    // }
    // println!("time in second = {:?}", SystemTime::now().duration_since(begin_time).unwrap().as_secs());
    // Get the key-value pair
    // let resp = client
    //     .kv()
    //     .range(RangeRequest::new(KeyRange::key(key)))
    //     .await?;
    // println!("Range Response: {:?}", resp);

    // Delete the key-valeu pair
    // let resp = client
    //     .kv()
    //     .delete(DeleteRequest::new(KeyRange::key(key)))
    //     .await?;
    // println!("Delete Response: {:?}", resp);

   Ok(())
}


async fn keep_alive_lease(client: &Client) -> Result<(), String> {
    //lease_time must longer then keep_alive (time)
    // let mut interval = tokio::time::interval(Duration::from_secs(keep_alive));
    // grant lease
    let lease_time = 10;
    let keep_alive = 5;
    let lease = retry_lease(client, lease_time).await?;
    let lease_id = lease.id();
    info!("got lease id = [{}]", lease_id);
    // One lease can be used by may key, So lease need only one for usually
    reg_keep_alive(client, keep_alive).await?;

    let reg_path = format!("/members/1");
    put_key2lease(client, lease_id, reg_path).await?;
        
    // let client = client.clone();
    let mut interval = tokio::time::interval(Duration::from_secs(keep_alive));
    loop {
        // lease for all keys one times
        let result = client
            .lease()
            .keep_alive(LeaseKeepAliveRequest::new(lease_id))
            .await;
        match result{
            Ok(_)=>debug!("keep alive is ok ..., wait for next time"),
            Err(e)=>{
                error!("{}", e);
                //NOTICE! retry is no usefull when connect is intrupted,must bo reconnect etcd
                return Err(format!("keep alive the lease id {} failed", lease_id));
            }
        }
        interval.tick().await;
    }

}

async fn retry_lease(
    client:&Client,
    lease_time:u64)->Result<LeaseGrantResponse, String>{
    for i in 0..=3{
        let lease_rst = client
        .lease()
        .grant(LeaseGrantRequest::new(Duration::from_secs(lease_time)))
        .await;
        match lease_rst {
            Ok(l)=> {return Ok(l)},
            Err(e)=> {
                error!("grant lease & retry [{}] times error: {}", i,e);
            }
        };
    }
    return Err("grant lease  failed!".into())
}

async fn reg_keep_alive(client:&Client, keep_alive:u64)->Result<(), String>{
    // watch keep alive event
    let result = client.lease().keep_alive_responses().await;
    let mut inbound = match result {
        Ok(i)=> i,
        Err(e)=> {
            error!("alive_responses error: {}", e);
            client.shutdown().await.unwrap_err();
            return Err("".into());
        }
    };
    let mut interval = tokio::time::interval(Duration::from_secs(keep_alive));
    info!("grant lease and keep alive ...");
    tokio::spawn(async move {
        loop {
            match inbound.next().await {
                Some(resp) => {
                    match resp {
                        //ignore ok response,but under debug model
                        Ok(o) => debug!("{:?}", o),
                        Err(e) => {
                            debug!("{:?}", e);
                            // If error got,like network to etcd is interrupted, etcd-rs retry auto
                            // nothing need to do, only waiting interval
                            break
                        }
                    }
                }
                None => {
                    debug!("keep alive response None!");
                    //nothing to do
                }
            };
            interval.tick().await;
            debug!("keep loop...");
        }
    });
    Ok(())
}

async fn put_key2lease(client:&Client,lease_id:u64, reg_path:String)->Result<(), String>{
    //retry put key for lease while connect is interupted, as some etcd node is down
    for i in 0..=3{
        // set lease for key
        //may panicked when connect lost,throw by etcd-rs crate
        let result = client
        .kv()
        .put({
            let mut req = PutRequest::new(reg_path.as_str(), format!("lease_id=[{}]", lease_id));
            req.set_lease(lease_id);
            req
        })
        .await;
        match result{
            Ok(_)=>{
                info!("service regist path = [{}] successfully with lease id = [{}]", reg_path, lease_id);
                return Ok(())
            },
            Err(e)=>{
                error!("put key [{}] with lease [{}] fail times [{}], error: {}", reg_path, lease_id, i, e);
            }
        };
        
    }
    return Err(format!("set lease {} for key {} failed!", lease_id, reg_path));
}