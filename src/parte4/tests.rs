use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Duration;

use super::mantenimiento::{simular_mantenimiento, ConfigMantenimiento, ResultadoMantenimiento};
use super::recepcion::{simular_recepcion, ConfigRecepcion, ResultadoRecepcion};

//Corre la simulación en otro hilo y espera a que termine.
//Si no termina antes del límite, se considera que hubo un deadlock.
fn ejecutar_con_limite<T: Send + 'static>(
    limite: Duration,
    simulacion: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(simulacion());
    });
    match rx.recv_timeout(limite) {
        Ok(resultado) => resultado,
        Err(RecvTimeoutError::Timeout) => {
            panic!("Posible deadlock: la simulación no terminó en {:?}", limite)
        }
        //El hilo terminó sin enviar el resultado: algún hilo de la simulación entró en pánico
        Err(RecvTimeoutError::Disconnected) => panic!("La simulación terminó con un error"),
    }
}

//Corre la zona de recepción y verifica que:
//- termina antes del límite (no hay deadlock)
//- cada paquete fue tomado exactamente una vez (ninguno se perdió ni se repitió)
//- la cinta nunca superó su capacidad
fn verificar_recepcion(config: ConfigRecepcion, limite: Duration) -> ResultadoRecepcion {
    let capacidad = config.capacidad;
    let total = config.total_paquetes;
    let resultado = ejecutar_con_limite(limite, move || simular_recepcion(config));

    let mut tomados = resultado.tomados.clone();
    tomados.sort();
    assert_eq!(tomados, (1..=total).collect::<Vec<_>>(), "Paquetes perdidos o repetidos");
    assert!(
        resultado.max_ocupacion <= capacidad,
        "La cinta tuvo {} paquetes con capacidad {}",
        resultado.max_ocupacion,
        capacidad
    );
    resultado
}

//Corre la estación de mantenimiento y verifica que:
//- termina antes del límite (no hay deadlock)
//- cada robot fue reparado exactamente una vez
//- nunca hubo más robots esperando que bahías
fn verificar_mantenimiento(config: ConfigMantenimiento, limite: Duration) -> ResultadoMantenimiento {
    let bahias = config.bahias;
    let total = config.total_robots;
    let resultado = ejecutar_con_limite(limite, move || simular_mantenimiento(config));

    let mut reparados = resultado.reparados.clone();
    reparados.sort();
    assert_eq!(reparados, (1..=total).collect::<Vec<_>>(), "Robots sin reparar o reparados dos veces");
    assert!(
        resultado.max_ocupacion <= bahias,
        "Hubo {} robots esperando con {} bahías",
        resultado.max_ocupacion,
        bahias
    );
    resultado
}

// ---------------------------------------------------------------------------
// Zona de Recepción
// ---------------------------------------------------------------------------

//Valores del enunciado: cinta de 10, 20 paquetes, 2 camiones y 3 robots, con demoras
#[test]
fn recepcion_valores_del_enunciado() {
    for _ in 0..5 {
        verificar_recepcion(
            ConfigRecepcion {
                capacidad: 10,
                total_paquetes: 20,
                camiones: 2,
                robots: 3,
                demora_camion_ms: 10..100,
                demora_robot_ms: 10..100,
            },
            Duration::from_secs(15),
        );
    }
}

//Sin demoras cada corrida tarda milisegundos, así que se puede repetir muchas veces
//para probar muchos intercalados distintos entre hilos
#[test]
fn recepcion_estres_sin_demoras() {
    for _ in 0..500 {
        verificar_recepcion(
            ConfigRecepcion {
                capacidad: 10,
                total_paquetes: 20,
                camiones: 2,
                robots: 3,
                demora_camion_ms: 0..0,
                demora_robot_ms: 0..0,
            },
            Duration::from_secs(5),
        );
    }
}

//Con capacidad 1 los camiones esperan casi siempre: máxima contención sobre not_full
#[test]
fn recepcion_cinta_de_un_lugar() {
    for _ in 0..200 {
        let resultado = verificar_recepcion(
            ConfigRecepcion {
                capacidad: 1,
                total_paquetes: 20,
                camiones: 2,
                robots: 3,
                demora_camion_ms: 0..0,
                demora_robot_ms: 0..0,
            },
            Duration::from_secs(5),
        );
        assert_eq!(resultado.max_ocupacion, 1);
    }
}

//Camiones rápidos y un robot lento: la cinta llega a llenarse y los camiones tienen que esperar
#[test]
fn recepcion_cinta_se_llena_con_robots_lentos() {
    for _ in 0..5 {
        let resultado = verificar_recepcion(
            ConfigRecepcion {
                capacidad: 5,
                total_paquetes: 30,
                camiones: 2,
                robots: 1,
                demora_camion_ms: 0..0,
                demora_robot_ms: 5..10,
            },
            Duration::from_secs(10),
        );
        assert_eq!(resultado.max_ocupacion, 5, "La cinta nunca llegó a llenarse");
    }
}

//Sin paquetes: los camiones terminan enseguida y los robots no deben quedar esperando
#[test]
fn recepcion_sin_paquetes() {
    for _ in 0..100 {
        let resultado = verificar_recepcion(
            ConfigRecepcion {
                capacidad: 10,
                total_paquetes: 0,
                camiones: 2,
                robots: 3,
                demora_camion_ms: 0..0,
                demora_robot_ms: 0..0,
            },
            Duration::from_secs(5),
        );
        assert!(resultado.tomados.is_empty());
    }
}

//Más robots que paquetes: algunos robots no toman nada y tienen que terminar igual
#[test]
fn recepcion_mas_robots_que_paquetes() {
    for _ in 0..200 {
        verificar_recepcion(
            ConfigRecepcion {
                capacidad: 10,
                total_paquetes: 3,
                camiones: 2,
                robots: 10,
                demora_camion_ms: 0..0,
                demora_robot_ms: 0..0,
            },
            Duration::from_secs(5),
        );
    }
}

//Muchos camiones compitiendo por una cinta chica con un solo robot
#[test]
fn recepcion_muchos_camiones_un_robot() {
    for _ in 0..200 {
        verificar_recepcion(
            ConfigRecepcion {
                capacidad: 2,
                total_paquetes: 50,
                camiones: 6,
                robots: 1,
                demora_camion_ms: 0..0,
                demora_robot_ms: 0..0,
            },
            Duration::from_secs(5),
        );
    }
}

//Estrés a gran escala: muchos hilos y mucho volumen en una sola corrida.
//Con 20 robots esperando en not_empty se prueba que no se pierdan despertares
//y que el notify_all final despierte a todos.
#[test]
fn recepcion_estres_gran_escala() {
    for _ in 0..3 {
        verificar_recepcion(
            ConfigRecepcion {
                capacidad: 10,
                total_paquetes: 10_000,
                camiones: 20,
                robots: 20,
                demora_camion_ms: 0..0,
                demora_robot_ms: 0..0,
            },
            Duration::from_secs(30),
        );
    }
}

#[test]
fn recepcion_sin_camiones() {
    let config =
        ConfigRecepcion {
            capacidad: 10,
            total_paquetes: 10,
            camiones: 0,
            robots: 3,
            demora_camion_ms: 0..0,
            demora_robot_ms: 0..0,
        };
    let limite = Duration::from_secs(5);
    let resultado = ejecutar_con_limite(limite, move || simular_recepcion(config));
    assert!(resultado.tomados.is_empty());
    assert_eq!(resultado.max_ocupacion, 0);
}

#[test]
fn recepcion_sin_capacidad() {
    let config =
        ConfigRecepcion {
            capacidad: 0,
            total_paquetes: 10,
            camiones: 10,
            robots: 3,
            demora_camion_ms: 0..0,
            demora_robot_ms: 0..0,
        };
    let limite = Duration::from_secs(5);
    let resultado = ejecutar_con_limite(limite, move || simular_recepcion(config));
    assert!(resultado.tomados.is_empty());
    assert_eq!(resultado.max_ocupacion, 0);
}

#[test]
fn recepcion_sin_robots() {
    let config =
        ConfigRecepcion {
            capacidad: 10,
            total_paquetes: 10,
            camiones: 10,
            robots: 0,
            demora_camion_ms: 0..0,
            demora_robot_ms: 0..0,
        };
    let limite = Duration::from_secs(5);
    let resultado = ejecutar_con_limite(limite, move || simular_recepcion(config));
    assert!(resultado.tomados.is_empty());
    assert_eq!(resultado.max_ocupacion, 0);
}
// ---------------------------------------------------------------------------
// Estación de Mantenimiento
// ---------------------------------------------------------------------------

//Valores del enunciado: 4 bahías y 10 robots, con demoras. Cada corrida tarda unos segundos.
#[test]
fn mantenimiento_valores_del_enunciado() {
    for _ in 0..3 {
        verificar_mantenimiento(
            ConfigMantenimiento {
                bahias: 4,
                total_robots: 10,
                llegada_ms: 100..2000,
                reparacion_ms: 200..500,
            },
            Duration::from_secs(30),
        );
    }
}

//Sin demoras: todos los robots llegan casi a la vez, muchas repeticiones
#[test]
fn mantenimiento_estres_sin_demoras() {
    for _ in 0..300 {
        verificar_mantenimiento(
            ConfigMantenimiento {
                bahias: 4,
                total_robots: 10,
                llegada_ms: 0..0,
                reparacion_ms: 0..0,
            },
            Duration::from_secs(5),
        );
    }
}

//Una sola bahía, robots que llegan juntos y reparaciones lentas: se fuerzan los rechazos.
//Los robots rechazados tienen que volver y ser reparados igual.
#[test]
fn mantenimiento_fuerza_rechazos() {
    for _ in 0..5 {
        let resultado = verificar_mantenimiento(
            ConfigMantenimiento {
                bahias: 1,
                total_robots: 10,
                llegada_ms: 0..5,
                reparacion_ms: 20..30,
            },
            Duration::from_secs(10),
        );
        assert!(resultado.rechazos > 0, "No hubo ningún rechazo");
    }
}

//Estrés con el taller saturado: 100 robots para 4 bahías.
//Los rechazados reintentan enseguida y compiten todo el tiempo por el lock con el mecánico;
//se verifica que el mecánico igual avanza y todos terminan reparados (sin inanición).
//La reparación dura 1-2 ms para que las bahías lleguen a llenarse aunque los hilos
//de los robots arranquen espaciados (por ejemplo, con otras pruebas corriendo en paralelo).
#[test]
fn mantenimiento_estres_taller_saturado() {
    for _ in 0..3 {
        let resultado = verificar_mantenimiento(
            ConfigMantenimiento {
                bahias: 4,
                total_robots: 100,
                llegada_ms: 0..0,
                reparacion_ms: 1..3,
            },
            Duration::from_secs(30),
        );
        assert!(resultado.rechazos > 0, "El taller nunca llegó a saturarse");
    }
}

//Si hay una bahía por robot nunca puede faltar lugar, así que no debe haber rechazos
#[test]
fn mantenimiento_sin_rechazos_con_bahias_suficientes() {
    for _ in 0..100 {
        let resultado = verificar_mantenimiento(
            ConfigMantenimiento {
                bahias: 10,
                total_robots: 10,
                llegada_ms: 0..0,
                reparacion_ms: 1..3,
            },
            Duration::from_secs(5),
        );
        assert_eq!(resultado.rechazos, 0);
    }
}

//Sin robots: el mecánico no debe quedarse dormido para siempre
#[test]
fn mantenimiento_sin_robots() {
    let resultado = verificar_mantenimiento(
        ConfigMantenimiento {
            bahias: 4,
            total_robots: 0,
            llegada_ms: 0..0,
            reparacion_ms: 0..0,
        },
        Duration::from_secs(5),
    );
    assert!(resultado.reparados.is_empty());
}

#[test]
fn mantenimiento_sin_bahias() {
    let config = ConfigMantenimiento {
            bahias: 0,
            total_robots: 3,
            llegada_ms: 0..0,
            reparacion_ms: 0..0,
        };
        let limite = Duration::from_secs(5);
    let resultado = ejecutar_con_limite(limite, move || simular_mantenimiento(config));
    assert!(resultado.reparados.is_empty());
    assert_eq!(resultado.max_ocupacion, 0);
    assert_eq!(resultado.rechazos, 0);
}