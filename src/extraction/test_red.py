import requests

print("1. Intentando conectar a la puerta principal de Spotify...")
respuesta = requests.get('https://api.spotify.com/v1/')

print(f"\n2. ¿A dónde nos han redirigido realmente?")
print(f"URL Final: {respuesta.url}")
print(f"Código de estado: {respuesta.status_code}")