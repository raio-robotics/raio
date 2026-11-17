use pyo3::prelude::*;

#[pyfunction]
fn add(left: u64, right: u64) -> u64 {
    raio::add(left, right)
}

#[pymodule(name = "raio")]
fn raio_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add, m)?)?;
    Ok(())
}
