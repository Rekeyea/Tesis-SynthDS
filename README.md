# Dataset Sintetico de Prueba para Monitoreo Remoto de Pacientes

Los comandos deberian ejecutarse en el siguiente orden: 

```bash
 uv run generate.py config.json output.csv
```

```bash
 uv run separate.py output.csv
```

```bash
 uv run process.py
```