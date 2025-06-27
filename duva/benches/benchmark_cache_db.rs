use criterion::Criterion;
use std::{
    cell::{RefCell, UnsafeCell},
    rc::{Rc, Weak},
};

struct NodeSafe {
    _value: String,
    prev: Option<Weak<RefCell<NodeSafe>>>,
    next: Option<Weak<RefCell<NodeSafe>>>,
}
struct NodePtr {
    _value: String,
    prev: Option<*mut NodePtr>,
    next: Option<*mut NodePtr>,
}
fn node_safe_move_front(
    front_org: &mut Option<Weak<RefCell<NodeSafe>>>,
    node_org: Weak<RefCell<NodeSafe>>,
) {
    let node = node_org.upgrade().unwrap();
    let mut node = node.borrow_mut();
    if let Some(next) = &node.next {
        let next = next.upgrade().unwrap();
        let mut next = next.borrow_mut();
        next.prev = node.prev.clone();
    }
    if let Some(prev) = &node.prev {
        let prev = prev.upgrade().unwrap();
        let mut prev = prev.borrow_mut();
        prev.next = node.next.clone();
    }
    let front = front_org.clone().unwrap();
    let front = front.upgrade().unwrap();
    let mut front = front.borrow_mut();
    front.prev = Some(node_org);
    node.prev = None;
    node.next = front_org.clone();
}
fn node_ptr_move_front(front_org: &mut Option<*mut NodePtr>, node_org: *mut NodePtr) {
    let node = unsafe { &mut *node_org };
    if let Some(next) = node.next {
        let next = unsafe { &mut *next };
        next.prev = node.prev;
    }
    if let Some(prev) = node.prev {
        let prev = unsafe { &mut *prev };
        prev.next = node.next;
    }
    let front = unsafe { &mut **front_org.as_mut().unwrap() };
    front.prev = Some(node_org);
    node.prev = None;
    node.next = *front_org;
}
pub fn bench_safe(c: &mut Criterion) {
    let origin: Vec<_> = (0..10000)
        .map(|x| Rc::new(RefCell::new(NodeSafe { _value: x.to_string(), prev: None, next: None })))
        .collect();
    for (i, node) in origin.iter().enumerate() {
        let mut node = node.borrow_mut();
        if i > 0 {
            node.prev = Some(Rc::downgrade(&origin[i - 1]));
        }
        if i < origin.len() - 1 {
            node.next = Some(Rc::downgrade(&origin[i + 1]));
        }
    }
    let mut front = Some(Rc::downgrade(&origin[0]));
    let ptr = origin.iter().map(Rc::downgrade).collect::<Vec<_>>();

    c.bench_function("Bench Cache Db Safe", |b| {
        b.iter(|| {
            let i = rand::random_range(1..10000);
            let node = ptr[i].clone();
            node_safe_move_front(&mut front, node);
        })
    });
}
pub fn bench_ptr(c: &mut Criterion) {
    let origin: Vec<_> = (0..10000)
        .map(|x| {
            Box::pin(UnsafeCell::new(NodePtr { _value: x.to_string(), prev: None, next: None }))
        })
        .collect();
    for (i, node) in origin.iter().enumerate() {
        let node = unsafe { &mut *node.get() };
        if i > 0 {
            node.prev = Some(origin[i - 1].get());
        }
        if i < origin.len() - 1 {
            node.next = Some(origin[i + 1].get());
        }
    }
    let mut front = Some(origin[0].get());

    c.bench_function("Bench Cache Db Ptr", |b| {
        b.iter(|| {
            let i = rand::random_range(1..10000);
            let node = origin[i].get();
            node_ptr_move_front(&mut front, node);
        })
    });
}
