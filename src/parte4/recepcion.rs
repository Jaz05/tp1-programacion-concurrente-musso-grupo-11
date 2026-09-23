use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use rand::Rng;

const BUFFER_SIZE: usize = 1;
const TOTAL_ITEMS: usize = 20;


pub fn zona_recepcion() {
    let buffer = Arc::new((
        Mutex::new((VecDeque::<usize>::new(), false)),
        Condvar::new(), // not_full
        Condvar::new(), // not_empty
    ));

    //Agregamos una lista finita de productos entregados por los dos camiones
    let lista_productos = Arc::new(Mutex::new((VecDeque::<usize>::new(), false)));
    let mut lista = lista_productos.lock().unwrap();
    for i in 1..=TOTAL_ITEMS {
        lista.0.push_back(i);
    }
    drop(lista);

    let mut camiones = vec![];
    for id in 1..=2 {
        
        let buffer_prod = Arc::clone(&buffer);
        let lista_prod = Arc::clone(&lista_productos);
        let camion = thread::spawn(move || {            
            let mut rng = rand::thread_rng();
            let (ref lock, ref not_full, ref not_empty) = *buffer_prod;
            
            loop{
                let mut lista = lista_prod.lock().unwrap();
                let mut state = lock.lock().unwrap();

                //El camión espera si la cinta está llena y no puede mandar más productos
                while state.0.len() >= BUFFER_SIZE { 
                    println!("La cinta esta llena, el camión {} espera", id);
                    state = not_full.wait(state).unwrap(); 
                }

                //Si el camión no tiene más productos para entregar termina
                if lista.0.is_empty(){
                    drop(lista);
                    break;
                }

                //El camión pone productos en la cinta
                let item = lista.0.pop_back().unwrap();
                state.0.push_back(item);
                println!(
                    "Camion {}: dejé paquete {}", id,
                    item
                );
                //Notifica a cualquiera de los robots que hay un producto en la cinta 
                not_empty.notify_one();
                drop(state);
                
                //Si no hay más productos termina
                if lista.0.is_empty(){
                    drop(lista);
                    break;
                }
                drop(lista);
                    
                thread::sleep(Duration::from_millis(rng.gen_range(10..100)));
            }

            let mut state = lock.lock().unwrap();
            state.1 = true;
            //Avisa a todos los robots para que salgan de sus bloqueos
            not_empty.notify_all(); 
            println!("Camion {} terminó de descargar", id);
        });
        camiones.push(camion);
    }

    let mut robots = vec![];
    for id in 1..=3 {
        
        let buffer_cons = Arc::clone(&buffer);

        let robot = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let (ref lock, ref not_full, ref not_empty) = *buffer_cons;

            loop {
                let mut state = lock.lock().unwrap(); // <-- dado como ejemplo

                while state.0.is_empty() {                    
                    if state.1 { println!("Robot {}: no hay más items para descargar.", id); return; }else{
                        println!("No hay nada para descargar, el robot {} espera", id);
                    }
                    //Espera a que lo notifiquen que hay un producto en la cinta
                    state = not_empty.wait(state).unwrap();
                }

                let item = state.0.pop_front().unwrap();

                println!(
                    "Robot {}: tomó paquete {}",
                    id, item
                );
                //Avisa a cualquiera de los camiones que ya hay lugar en la cinta
                not_full.notify_one();

                drop(state);
                thread::sleep(Duration::from_millis(rng.gen_range(10..100)));
            }
        });
        robots.push(robot);
    }

    for camion in camiones {
        camion.join().unwrap();
    }
    
    for robot in robots {
        robot.join().unwrap();
    }
}
