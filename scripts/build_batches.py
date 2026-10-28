import pandas as pd

def build_batches():
    # Fechas
    df_f = pd.read_csv('datos_dw_csv/dim_fecha.csv')
    sql_f = 'INSERT INTO `hyperflix-analytics.hyperflix_dw.dim_fecha` (fecha_id, anio, mes, dia, trimestre, nombre_mes, dia_semana) VALUES\n'
    vals_f = [f"(DATE '{r.fecha_id}', {r.anio}, {r.mes}, {r.dia}, {r.trimestre}, '{r.nombre_mes}', '{r.dia_semana}')" for _, r in df_f.iterrows()]
    sql_f += ',\n'.join(vals_f) + ';\n'
    with open('datos_dw_csv/poblar_fechas.sql', 'w', encoding='utf-8') as f:
        f.write(sql_f)
    print('poblar_fechas.sql generado')

    # Fact en bloques de 1500 filas
    df_fact = pd.read_csv('datos_dw_csv/fact_reproducciones.csv')
    batch_size = 1500
    num_batches = (len(df_fact) + batch_size - 1) // batch_size
    print(f'Generando {num_batches} bloques para fact_reproducciones...')
    
    with open('datos_dw_csv/poblar_fact.sql', 'w', encoding='utf-8') as f:
        for b in range(num_batches):
            chunk = df_fact.iloc[b*batch_size : (b+1)*batch_size]
            sql = 'INSERT INTO `hyperflix-analytics.hyperflix_dw.fact_reproducciones` (evento_id, contenido_id, usuario_id, dispositivo_id, fecha_id, minutos_vistos, duracion_total_min, completitud_pct, latencia_ms, buffering_count, abandono_prematuro, score_qos, fecha_evento) VALUES\n'
            vals = []
            for _, r in chunk.iterrows():
                vals.append(f"('{r.evento_id}', '{r.contenido_id}', '{r.usuario_id}', '{r.dispositivo_id}', DATE '{r.fecha_id}', {r.minutos_vistos}, {r.duracion_total_min}, {r.completitud_pct}, {r.latencia_ms}, {r.buffering_count}, {str(r.abandono_prematuro).upper()}, {r.score_qos}, TIMESTAMP '{r.fecha_evento}')")
            sql += ',\n'.join(vals) + ';\n\n'
            f.write(sql)
    print('poblar_fact.sql generado exitosamente')

if __name__ == '__main__':
    build_batches()
