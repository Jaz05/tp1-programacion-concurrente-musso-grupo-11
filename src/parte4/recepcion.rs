use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use super::demora_aleatoria;

//Parámetros de la simulación, para poder probar otros tamaños y tiempos
pub struct ConfigRecepcion {
    pub capacidad: usize,
    pub total_paquetes: usize,
    pub camiones: usize,
    pub robots: usize,
    pub demora_camion_ms: Range<u64>,
    pub demora_robot_ms: Range<u64>,
}

//Lo que pasó durante la simulación, para poder verificarlo en las pruebas
pub struct ResultadoRecepcion {
    //Paquetes que tomaron los robots, en cualquier orden
    pub tomados: Vec<usize>,
    //Máxima cantidad de paquetes que hubo en la cinta al mismo tiempo
    pub max_ocupacion: usize,
}

//Simulación con los valores del enunciado
pub fn zona_recepcion() {
    let config = ConfigRecepcion {
        capacidad: 10,
        total_paquetes: 20,
        camiones: 2,
        robots: 3,
        demora_camion_ms: 10..100,
        demora_robot_ms: 10..100,
    };
    let (total_paquetes, capacidad) = (config.total_paquetes, config.capacidad);

    let resultado = simular_recepcion(config);
    println!(
        "\nResumen recepción: {} paquetes tomados de {}, ocupación máxima de la cinta: {}/{}\n",
        resultado.tomados.len(), total_paquetes, resultado.max_ocupacion, capacidad
    );
}

pub fn simular_recepcion(config: ConfigRecepcion) -> ResultadoRecepcion {
    //Con capacidad 0 o sin camiones/robots la simulación nunca terminaría
    assert!(config.capacidad > 0 && config.camiones > 0 && config.robots > 0);

    let buffer = Arc::new((
        Mutex::new((VecDeque::<usize>::new(), false)),
        Condvar::new(), // not_full
        Condvar::new(), // not_empty
    ));

    //Agregamos una lista finita de productos entregados por los camiones
    let lista_productos = Arc::new(Mutex::new((VecDeque::<usize>::new(), false)));
    let mut lista = lista_productos.lock().unwrap();
    for i in 1..=config.total_paquetes {
        lista.0.push_back(i);
    }
    drop(lista);

    let mut camiones = vec![];
    for id in 1..=config.camiones {

        let buffer_prod = Arc::clone(&buffer);
        let lista_prod = Arc::clone(&lista_productos);
        let capacidad = config.capacidad;
        let demora = config.demora_camion_ms.clone();
        let camion = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let (ref lock, ref not_full, ref not_empty) = *buffer_prod;
            //Para los tests
            let mut max_ocupacion = 0;

            loop{
                let mut lista = lista_prod.lock().unwrap();
                let mut state = lock.lock().unwrap();

                //El camión espera si la cinta está llena y no puede mandar más productos
                while state.0.len() >= capacidad {
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
                max_ocupacion = max_ocupacion.max(state.0.len());
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

                demora_aleatoria(&mut rng, &demora);
            }

            let mut state = lock.lock().unwrap();
            state.1 = true;
            //Avisa a todos los robots para que salgan de sus bloqueos
            not_empty.notify_all();
            println!("Camion {} terminó de descargar", id);
            max_ocupacion
        });
        camiones.push(camion);
    }

    let mut robots = vec![];
    for id in 1..=config.robots {

        let buffer_cons = Arc::clone(&buffer);
        let demora = config.demora_robot_ms.clone();

        let robot = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let (ref lock, ref not_full, ref not_empty) = *buffer_cons;
            //Para los tests
            let mut tomados = vec![];

            loop {
                let mut state = lock.lock().unwrap(); // <-- dado como ejemplo

                while state.0.is_empty() {
                    if state.1 { println!("Robot {}: no hay más items para descargar.", id); return tomados; }else{
                        println!("No hay nada para descargar, el robot {} espera", id);
                    }
                    //Espera a que lo notifiquen que hay un producto en la cinta
                    state = not_empty.wait(state).unwrap();
                }

                let item = state.0.pop_front().unwrap();
                tomados.push(item);

                println!(
                    "Robot {}: tomó paquete {}",
                    id, item
                );
                //Avisa a cualquiera de los camiones que ya hay lugar en la cinta
                not_full.notify_one();

                drop(state);
                demora_aleatoria(&mut rng, &demora);
            }
        });
        robots.push(robot);
    }

    //Info relevante para los tests
    let mut max_ocupacion = 0;
    for camion in camiones {
        max_ocupacion = max_ocupacion.max(camion.join().unwrap());
    }

    let mut tomados = vec![];
    for robot in robots {
        tomados.extend(robot.join().unwrap());
    }

    ResultadoRecepcion { tomados, max_ocupacion }
}
