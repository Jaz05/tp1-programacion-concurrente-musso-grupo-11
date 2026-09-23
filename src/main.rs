mod parte4;

use crate::parte4::recepcion::zona_recepcion;
use crate::parte4::mantenimiento::estacion_mantenimiento;  

fn main() {
    println!("Ejercicio parte 4: Zona de recepción:");
    zona_recepcion();

    println!("Ejercicio parte 4: Estación de mantenimiento:");
    estacion_mantenimiento();
    // TODO: parte4::recepcion::simular();
    // TODO: parte4::mantenimiento::simular();
}
