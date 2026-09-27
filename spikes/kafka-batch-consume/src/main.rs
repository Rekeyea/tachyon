//! Spike: ¿qué variante de consumo entrega mensajes con un consumidor nativo?
//!
//! Uso: kafka-batch-consume-spike <brokers> <topic> <modo> <segundos>
//!   modo = base   (rdkafka-rust BaseConsumer::poll, referencia conocida)
//!        | msg    (rd_kafka_consumer_poll, mensaje a mensaje, FFI)
//!        | batch  (rd_kafka_consume_batch_queue, lotes, FFI)
//!        | batch-no-cb (batch SIN rebalance_cb — reproduce el unassign forzado)

use std::ffi::CString;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientConfig;
use rdkafka_sys as rdsys;

unsafe extern "C" fn rebalance_cb(
    rk: *mut rdsys::rd_kafka_t,
    err: rdsys::rd_kafka_resp_err_t,
    partitions: *mut rdsys::rd_kafka_topic_partition_list_t,
    _opaque: *mut std::ffi::c_void,
) {
    let n = if partitions.is_null() { 0 } else { (*partitions).cnt };
    eprintln!("[cb] rebalance err={:?} n={}", err, n);
    if err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
        rdsys::rd_kafka_assign(rk, partitions);
    } else {
        rdsys::rd_kafka_assign(rk, std::ptr::null_mut());
    }
}

fn base_config(brokers: &str, group: &str) -> ClientConfig {
    let mut cc = ClientConfig::new();
    cc.set("bootstrap.servers", brokers);
    cc.set("group.id", group);
    cc.set("auto.offset.reset", "earliest");
    cc.set("enable.auto.commit", "false");
    cc.set("fetch.min.bytes", "524288");
    cc.set("fetch.wait.max.ms", "1000");
    cc
}

fn native_consumer(config: &ClientConfig, topic: &str, with_cb: bool) -> *mut rdsys::rd_kafka_t {
    let conf = config.create_native_config().expect("native config");
    if with_cb {
        unsafe { rdsys::rd_kafka_conf_set_rebalance_cb(conf.ptr(), Some(rebalance_cb)) };
    }
    let mut errbuf = [0 as std::ffi::c_char; 512];
    let rk = unsafe {
        rdsys::rd_kafka_new(
            rdsys::rd_kafka_type_t::RD_KAFKA_CONSUMER,
            conf.ptr(),
            errbuf.as_mut_ptr(),
            errbuf.len(),
        )
    };
    if rk.is_null() {
        panic!("rd_kafka_new: {}", unsafe {
            CStr_bytes(&errbuf)
        });
    }
    std::mem::forget(conf);
    let err = unsafe { rdsys::rd_kafka_poll_set_consumer(rk) };
    assert_eq!(err, rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
    let topic_c = CString::new(topic).unwrap();
    let topics = unsafe { rdsys::rd_kafka_topic_partition_list_new(1) };
    unsafe { rdsys::rd_kafka_topic_partition_list_add(topics, topic_c.as_ptr(), -1) };
    let err = unsafe { rdsys::rd_kafka_subscribe(rk, topics) };
    unsafe { rdsys::rd_kafka_topic_partition_list_destroy(topics) };
    assert_eq!(err, rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
    rk
}

unsafe fn CStr_bytes(buf: &[std::ffi::c_char]) -> String {
    std::ffi::CStr::from_ptr(buf.as_ptr()).to_string_lossy().into_owned()
}

fn run_base(brokers: &str, topic: &str, secs: u64) {
    let group = format!("spike-base-{}", std::process::id());
    let consumer: BaseConsumer = base_config(brokers, &group).create().expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut n = 0u64;
    let t0 = Instant::now();
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(_)) => n += 1,
            Some(Err(e)) => eprintln!("[base] poll error: {e}"),
            None => {}
        }
    }
    let el = t0.elapsed().as_secs_f64();
    eprintln!("[base] {n} mensajes en {el:.1}s = {:.0} msg/s", n as f64 / el);
}

fn run_msg(config: &ClientConfig, topic: &str, secs: u64, with_cb: bool) {
    let rk = native_consumer(config, topic, with_cb);
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut n = 0u64;
    let t0 = Instant::now();
    unsafe {
        while Instant::now() < deadline {
            let rkm = rdsys::rd_kafka_consumer_poll(rk, 500);
            if rkm.is_null() {
                continue;
            }
            let msg = &*rkm;
            if msg.err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                eprintln!("[msg] err en mensaje: {:?}", msg.err);
            } else {
                n += 1;
            }
            rdsys::rd_kafka_message_destroy(rkm);
        }
        rdsys::rd_kafka_consumer_close(rk);
        rdsys::rd_kafka_destroy(rk);
    }
    let el = t0.elapsed().as_secs_f64();
    eprintln!("[msg] {n} mensajes en {el:.1}s = {:.0} msg/s", n as f64 / el);
}

fn run_batch(config: &ClientConfig, topic: &str, secs: u64, with_cb: bool) {
    let rk = native_consumer(config, topic, with_cb);
    let queue = unsafe { rdsys::rd_kafka_queue_get_consumer(rk) };
    assert!(!queue.is_null());
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut n = 0u64;
    let t0 = Instant::now();
    let mut rkms: Vec<*mut rdsys::rd_kafka_message_t> = Vec::with_capacity(8192);
    unsafe {
        while Instant::now() < deadline {
            let got = rdsys::rd_kafka_consume_batch_queue(queue, 500, rkms.as_mut_ptr(), 8192);
            if got < 0 {
                eprintln!("[batch] consume_batch_queue devolvió {got}");
                break;
            }
            for &rkm in rkms.iter().take(got as usize) {
                let msg = &*rkm;
                if msg.err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    eprintln!("[batch] err en mensaje: {:?}", msg.err);
                } else {
                    n += 1;
                }
                rdsys::rd_kafka_message_destroy(rkm);
            }
        }
        rdsys::rd_kafka_queue_destroy(queue);
        rdsys::rd_kafka_consumer_close(rk);
        rdsys::rd_kafka_destroy(rk);
    }
    let el = t0.elapsed().as_secs_f64();
    eprintln!("[batch] {n} mensajes en {el:.1}s = {:.0} msg/s", n as f64 / el);
}

/// Batch sobre la cola principal (rk_rep, forwardeada a rkcg_q).
fn run_batch_main(config: &ClientConfig, topic: &str, secs: u64) {
    let rk = native_consumer(config, topic, true);
    let queue = unsafe { rdsys::rd_kafka_queue_get_main(rk) };
    assert!(!queue.is_null());
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut n = 0u64;
    let t0 = Instant::now();
    let mut rkms: Vec<*mut rdsys::rd_kafka_message_t> = Vec::with_capacity(8192);
    unsafe {
        while Instant::now() < deadline {
            let got = rdsys::rd_kafka_consume_batch_queue(queue, 500, rkms.as_mut_ptr(), 8192);
            if got < 0 {
                eprintln!("[batch-main] error {got}");
                break;
            }
            for &rkm in rkms.iter().take(got as usize) {
                let msg = &*rkm;
                if msg.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    n += 1;
                }
                rdsys::rd_kafka_message_destroy(rkm);
            }
        }
        rdsys::rd_kafka_queue_destroy(queue);
        rdsys::rd_kafka_consumer_close(rk);
        rdsys::rd_kafka_destroy(rk);
    }
    let el = t0.elapsed().as_secs_f64();
    eprintln!("[batch-main] {n} mensajes en {el:.1}s = {:.0} msg/s", n as f64 / el);
}

/// Batch sobre la cola de la partición 0 + consumer_poll(0) para control.
fn run_batch_part(config: &ClientConfig, topic: &str, secs: u64) {
    let rk = native_consumer(config, topic, true);
    let topic_c = CString::new(topic).unwrap();
    // Esperar la asignación: consumer_poll sirve el op de rebalance.
    let deadline_assign = Instant::now() + Duration::from_secs(10);
    let pq = loop {
        let q = unsafe { rdsys::rd_kafka_queue_get_partition(rk, topic_c.as_ptr(), 0) };
        if !q.is_null() {
            break q;
        }
        unsafe {
            let rkm = rdsys::rd_kafka_consumer_poll(rk, 200);
            if !rkm.is_null() {
                rdsys::rd_kafka_message_destroy(rkm);
            }
        }
        assert!(Instant::now() < deadline_assign, "sin cola de partición");
    };
    eprintln!("[batch-part] cola de partición obtenida");
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut n = 0u64;
    let t0 = Instant::now();
    let mut rkms: Vec<*mut rdsys::rd_kafka_message_t> = Vec::with_capacity(8192);
    unsafe {
        while Instant::now() < deadline {
            // Control: drena ops no-datos de la cola del consumidor.
            loop {
                let rkm = rdsys::rd_kafka_consumer_poll(rk, 0);
                if rkm.is_null() {
                    break;
                }
                let msg = &*rkm;
                if msg.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    n += 1;
                }
                rdsys::rd_kafka_message_destroy(rkm);
            }
            let got = rdsys::rd_kafka_consume_batch_queue(pq, 200, rkms.as_mut_ptr(), 8192);
            if got < 0 {
                eprintln!("[batch-part] error {got}");
                break;
            }
            for &rkm in rkms.iter().take(got as usize) {
                let msg = &*rkm;
                if msg.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    n += 1;
                }
                rdsys::rd_kafka_message_destroy(rkm);
            }
        }
        rdsys::rd_kafka_queue_destroy(pq);
        rdsys::rd_kafka_consumer_close(rk);
        rdsys::rd_kafka_destroy(rk);
    }
    let el = t0.elapsed().as_secs_f64();
    eprintln!("[batch-part] {n} mensajes en {el:.1}s = {:.0} msg/s", n as f64 / el);
}

/// Batch SIN grupo: asignación manual de la partición 0, cola principal.
fn run_batch_manual(config: &ClientConfig, topic: &str, secs: u64) {
    // Sin subscribe: el grupo existe (group.id) pero no hay protocolo de
    // grupo; la asignación es manual vía rd_kafka_assign.
    let conf = config.create_native_config().expect("native config");
    let mut errbuf = [0 as std::ffi::c_char; 512];
    let rk = unsafe {
        rdsys::rd_kafka_new(
            rdsys::rd_kafka_type_t::RD_KAFKA_CONSUMER,
            conf.ptr(),
            errbuf.as_mut_ptr(),
            errbuf.len(),
        )
    };
    assert!(!rk.is_null());
    std::mem::forget(conf);
    let topic_c = CString::new(topic).unwrap();
    unsafe {
        let tpl = rdsys::rd_kafka_topic_partition_list_new(1);
        let elem = rdsys::rd_kafka_topic_partition_list_add(tpl, topic_c.as_ptr(), 0);
        (*elem).offset = 0; // desde el principio
        let err = rdsys::rd_kafka_assign(rk, tpl);
        rdsys::rd_kafka_topic_partition_list_destroy(tpl);
        assert_eq!(err, rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
    }
    let queue = unsafe { rdsys::rd_kafka_queue_get_main(rk) };
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut n = 0u64;
    let t0 = Instant::now();
    let mut rkms: Vec<*mut rdsys::rd_kafka_message_t> = Vec::with_capacity(8192);
    unsafe {
        while Instant::now() < deadline {
            let got = rdsys::rd_kafka_consume_batch_queue(queue, 500, rkms.as_mut_ptr(), 8192);
            if got < 0 {
                eprintln!("[batch-manual] error {got}");
                break;
            }
            for &rkm in rkms.iter().take(got as usize) {
                let msg = &*rkm;
                if msg.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    n += 1;
                }
                rdsys::rd_kafka_message_destroy(rkm);
            }
        }
        rdsys::rd_kafka_queue_destroy(queue);
        rdsys::rd_kafka_consumer_close(rk);
        rdsys::rd_kafka_destroy(rk);
    }
    let el = t0.elapsed().as_secs_f64();
    eprintln!("[batch-manual] {n} mensajes en {el:.1}s = {:.0} msg/s", n as f64 / el);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let brokers = &args[1];
    let topic = &args[2];
    let mode = &args[3];
    let secs: u64 = args[4].parse().unwrap();
    match mode.as_str() {
        "base" => run_base(brokers, topic, secs),
        "msg" => {
            let group = format!("spike-msg-{}", std::process::id());
            run_msg(&base_config(brokers, &group), topic, secs, true)
        }
        "batch" => {
            let group = format!("spike-batch-{}", std::process::id());
            run_batch(&base_config(brokers, &group), topic, secs, true)
        }
        "batch-no-cb" => {
            let group = format!("spike-batchnocb-{}", std::process::id());
            run_batch(&base_config(brokers, &group), topic, secs, false)
        }
        "batch-main" => {
            let group = format!("spike-batchmain-{}", std::process::id());
            run_batch_main(&base_config(brokers, &group), topic, secs)
        }
        "batch-part" => {
            let group = format!("spike-batchpart-{}", std::process::id());
            run_batch_part(&base_config(brokers, &group), topic, secs)
        }
        "batch-manual" => {
            let group = format!("spike-batchmanual-{}", std::process::id());
            run_batch_manual(&base_config(brokers, &group), topic, secs)
        }
        other => panic!("modo desconocido: {other}"),
    }
}
