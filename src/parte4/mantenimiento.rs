use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use rand::Rng;

const BAHIAS_ESPERA: usize = 4;
const TOTAL_ROBOTS: usize = 10;

struct Taller {
    esperando: VecDeque<usize>,   
    robots_restantes: usize,  
}

pub fn estacion_mantenimiento() {
    let taller = Arc::new((
        Mutex::new(Taller {
            esperando: VecDeque::new(),
            robots_restantes: TOTAL_ROBOTS,
        }),
        Condvar::new()
    ));

    let taller_m = Arc::clone(&taller);
    let mecanico = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let (ref lock, ref hay_robot) = *taller_m;

        loop {
            let mut state = lock.lock().unwrap();
            
            //El mecánico revisa las bahías de espera
            while state.esperando.is_empty() {
                if state.robots_restantes == 0 {
                    println!("Mecanico: No hay más robots para reparar, cierro el taller");
                    return;
                }
                println!("Mecanico: no hay robots para arreglar, me duermo...");
                //Se despierta cuando hacen el notify a hay_robot
                state = hay_robot.wait(state).unwrap();
            }

            //El mecánico toma uno de los robots de la bahía
            let robot_id = state.esperando.pop_front().unwrap();

            println!(
                "Mecánico: reparando al robot {}. (esperando: {})",
                robot_id,
                state.esperando.len()
            );

            //Libera el lock mientras arregla al robot
            drop(state);

            //Simula una demora arreglando al robot
            let duracion = rng.gen_range(200..500);
            thread::sleep(Duration::from_millis(duracion));

            let mut state = lock.lock().unwrap();

            //Un robot menos para arreglar
            state.robots_restantes -= 1;
            println!(
                "Mecánico: terminé con el robot {}. (restantes: {})",
                robot_id, state.robots_restantes
            );
            drop(state);
        }
    });

    let mut robots = vec![];
    for id in 1..=TOTAL_ROBOTS {
        let taller_r = Arc::clone(&taller);

        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            loop{

                // Retraso de los robots en llegar al taller
                thread::sleep(Duration::from_millis(rng.gen_range(100..2000)));

                let (ref lock, ref hay_robot) = *taller_r;
                let mut state = lock.lock().unwrap();

                println!("Robot {}: llegó al taller.", id);

                //Si no hay lugar en la bahía de espera, se va y regresa luego
                if state.esperando.len() >= BAHIAS_ESPERA {
                    println!("Robot {} rechazado, vuelve a trabajar.", id);
                    //El enunciado dice que el robot "volverá más tarde", entonces vuelve a iterar el loop
                    continue;
                }

                //El robot ingresa en la bahía
                state.esperando.push_back(id);
                println!("Robot {}: espero en la bahía. (ocupadas: {}/{})",
                        id, state.esperando.len(), BAHIAS_ESPERA);

                //Notifica al meçánico para que se despierte
                hay_robot.notify_one();
                drop(state);
                break;
            }
        });
        robots.push(handle);
    }

    for c in robots {
        c.join().unwrap();
    }
    mecanico.join().unwrap();

    println!("\nTaller cerrado.");
}