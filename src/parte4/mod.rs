use std::ops::Range;
use std::thread;
use std::time::Duration;

use rand::Rng;

pub mod recepcion;
pub mod mantenimiento;

#[cfg(test)]
mod tests;

//Duerme un tiempo al azar dentro del rango (en milisegundos).
//Con un rango vacío (ej. 0..0) no duerme, solo cede la CPU a otro hilo.
fn demora_aleatoria(rng: &mut impl Rng, rango_ms: &Range<u64>) {
    if rango_ms.is_empty() {
        thread::yield_now();
    } else {
        thread::sleep(Duration::from_millis(rng.gen_range(rango_ms.clone())));
    }
}
