import pandas as pd

def build_sql():
    # 1. dim_dispositivo
    df_d = pd.read_csv('datos_dw_csv/dim_dispositivo.csv')
    sql_d = 'INSERT INTO `hyperflix-analytics.hyperflix_dw.dim_dispositivo` (dispositivo_id, tipo_dispositivo, categoria) VALUES\n'
    vals_d = [f"('{r.dispositivo_id}', '{r.tipo_dispositivo}', '{r.categoria}')" for _, r in df_d.iterrows()]
    sql_d += ',\n'.join(vals_d) + ';\n'
    
    # 2. dim_contenido
    df_c = pd.read_csv('datos_dw_csv/dim_contenido.csv')
    sql_c = 'INSERT INTO `hyperflix-analytics.hyperflix_dw.dim_contenido` (contenido_id, titulo, genero_primario, generos, anio_estreno, duracion_min, rating_promedio, idioma) VALUES\n'
    vals_c = []
    for _, r in df_c.iterrows():
        tit = str(r['titulo']).replace("'", "\\'")
        gen_prim = str(r['genero_primario']).replace("'", "\\'")
        gens = str(r['generos']).replace("'", "\\'")
        idioma = str(r['idioma']).replace("'", "\\'")
        vals_c.append(f"('{r.contenido_id}', '{tit}', '{gen_prim}', '{gens}', {int(r.anio_estreno)}, {float(r.duracion_min)}, {float(r.rating_promedio)}, '{idioma}')")
    sql_c += ',\n'.join(vals_c) + ';\n'

    # 3. dim_usuario
    df_u = pd.read_csv('datos_dw_csv/dim_usuario.csv')
    sql_u = 'INSERT INTO `hyperflix-analytics.hyperflix_dw.dim_usuario` (usuario_id, pais, plan, dispositivo) VALUES\n'
    vals_u = []
    for _, r in df_u.iterrows():
        vals_u.append(f"('{r.usuario_id}', '{r.pais}', '{r.plan}', '{r.dispositivo}')")
    sql_u += ',\n'.join(vals_u) + ';\n'

    with open('datos_dw_csv/poblar_dimensiones.sql', 'w', encoding='utf-8') as f:
        f.write(sql_d + '\n' + sql_c + '\n' + sql_u)
    print('poblar_dimensiones.sql creado exitosamente')

if __name__ == '__main__':
    build_sql()
