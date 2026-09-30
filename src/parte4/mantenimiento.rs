use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use super::demora_aleatoria;

struct Taller {
    esperando: VecDeque<usize>,
    robots_restantes: usize,
    //Máxima cantidad de bahías ocupadas al mismo tiempo, para verificarlo en las pruebas
    max_ocupacion: usize,
}

//Parámetros de la simulación, para poder probar otros tamaños y tiempos
pub struct ConfigMantenimiento {
    pub bahias: usize,
    pub total_robots: usize,
    pub llegada_ms: Range<u64>,
    pub reparacion_ms: Range<u64>,
}

//Lo que pasó durante la simulación, para poder verificarlo en las pruebas
pub struct ResultadoMantenimiento {
    //Robots que reparó el mecánico, en el orden en que los reparó
    pub reparados: Vec<usize>,
    //Cantidad de veces que un robot fue rechazado por no haber bahías libres
    pub rechazos: usize,
    //Máxima cantidad de bahías ocupadas al mismo tiempo
    pub max_ocupacion: usize,
}

//Simulación con los valores del enunciado
pub fn estacion_mantenimiento() {
    let config = ConfigMantenimiento {
        bahias: 4,
        total_robots: 10,
        llegada_ms: 100..2000,
        reparacion_ms: 200..500,
    };
    let (total_robots, bahias) = (config.total_robots, config.bahias);

    let resultado = simular_mantenimiento(config);
    println!(
        "Resumen mantenimiento: {} robots reparados de {}, {} rechazos, ocupación máxima de bahías: {}/{}",
        resultado.reparados.len(), total_robots, resultado.rechazos, resultado.max_ocupacion, bahias
    );
}

pub fn simular_mantenimiento(config: ConfigMantenimiento) -> ResultadoMantenimiento {
    if config.bahias == 0{
        return ResultadoMantenimiento{reparados: vec![], rechazos: 0, max_ocupacion: 0};
    }
    //Sin bahías los robots serían rechazados para siempre
    assert!(config.bahias > 0);

    let taller = Arc::new((
        Mutex::new(Taller {
            esperando: VecDeque::new(),
            robots_restantes: config.total_robots,
            max_ocupacion: 0,
        }),
        Condvar::new()
    ));

    let taller_m = Arc::clone(&taller);
    let reparacion = config.reparacion_ms.clone();
    let mecanico = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let (ref lock, ref hay_robot) = *taller_m;
        //Para tests
        let mut reparados = vec![];

        loop {
            let mut state = lock.lock().unwrap();

            //El mecánico revisa las bahías de espera
            while state.esperando.is_empty() {
                if state.robots_restantes == 0 {
                    println!("Mecanico: No hay más robots para reparar, cierro el taller");
                    return reparados;
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
            demora_aleatoria(&mut rng, &reparacion);

            let mut state = lock.lock().unwrap();

            //Un robot menos para arreglar
            state.robots_restantes -= 1;
            reparados.push(robot_id);
            println!(
                "Mecánico: terminé con el robot {}. (restantes: {})",
                robot_id, state.robots_restantes
            );
            drop(state);
        }
    });

    let mut robots = vec![];
    for id in 1..=config.total_robots {
        let taller_r = Arc::clone(&taller);
        let bahias = config.bahias;
        let llegada = config.llegada_ms.clone();

        let handle = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            //Para tests
            let mut rechazos = 0;
            loop{

                // Retraso de los robots en llegar al taller
                demora_aleatoria(&mut rng, &llegada);

                let (ref lock, ref hay_robot) = *taller_r;
                let mut state = lock.lock().unwrap();

                println!("Robot {}: llegó al taller.", id);

                //Si no hay lugar en la bahía de espera, se va y regresa luego
                if state.esperando.len() >= bahias {
                    println!("Robot {} rechazado, vuelve a trabajar.", id);
                    rechazos += 1;
                    //El enunciado dice que el robot "volverá más tarde", entonces vuelve a iterar el loop
                    continue;
                }

                //El robot ingresa en la bahía
                state.esperando.push_back(id);
                state.max_ocupacion = state.max_ocupacion.max(state.esperando.len());
                println!("Robot {}: espero en la bahía. (ocupadas: {}/{})",
                        id, state.esperando.len(), bahias);

                //Notifica al mecánico para que se despierte
                hay_robot.notify_one();
                drop(state);
                break;
            }
            rechazos
        });
        robots.push(handle);
    }

    //Info util para los tests
    let mut rechazos = 0;
    for c in robots {
        rechazos += c.join().unwrap();
    }
    let reparados = mecanico.join().unwrap();

    println!("\nTaller cerrado.");

    let max_ocupacion = taller.0.lock().unwrap().max_ocupacion;
    ResultadoMantenimiento { reparados, rechazos, max_ocupacion }
}
