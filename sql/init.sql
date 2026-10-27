-- Dimensión de Clientes
-- Decisión de negocio: Incluiremos las transacciones sin ID asignándolas a un cliente 'UNKNOWN' para no perder el revenue de esas ventas.
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_type VARCHAR(50) -- Ej: 'Registered', 'Unknown'
);

-- Dimensión de Productos
-- Los códigos irán en mayúsculas y sin espacios. La descripción será la canónica (la más frecuente).
CREATE TABLE IF NOT EXISTS dim_products (
    product_code VARCHAR(50) PRIMARY KEY,
    canonical_description VARCHAR(255),
    category VARCHAR(100)
);

-- Hechos de Ventas
-- Solo ingresarán registros con cantidad > 0 y precio > 0.
CREATE TABLE IF NOT EXISTS fact_sales (
    invoice_no VARCHAR(50),
    product_code VARCHAR(50) REFERENCES dim_products(product_code),
    customer_id VARCHAR(50) REFERENCES dim_customers(customer_id),
    invoice_date TIMESTAMP, -- Estandarizado a UTC
    country VARCHAR(100),
    quantity INTEGER,
    unit_price NUMERIC(12,4),
    gross_revenue NUMERIC(12,4),
    -- Clave primaria compuesta para garantizar idempotencia en la carga
    PRIMARY KEY (invoice_no, product_code, invoice_date) 
);

-- Hechos de Devoluciones / Ajustes
-- Registros con cantidad <= 0. La cantidad se guardará en valor absoluto para facilitar cálculos.
CREATE TABLE IF NOT EXISTS fact_returns (
    invoice_no VARCHAR(50),
    product_code VARCHAR(50) REFERENCES dim_products(product_code),
    customer_id VARCHAR(50) REFERENCES dim_customers(customer_id),
    return_date TIMESTAMP, -- Estandarizado a UTC
    country VARCHAR(100),
    quantity INTEGER, 
    unit_price NUMERIC(12,4),
    return_amount NUMERIC(12,4),
    PRIMARY KEY (invoice_no, product_code, return_date)
);

-- Log de Rechazos
-- Para registros con precio <= 0 u otros errores críticos de calidad.
CREATE TABLE IF NOT EXISTS log_rejections (
    id SERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    record_data TEXT,
    rejection_reason VARCHAR(255),
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);