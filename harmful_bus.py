import trio
import trio_websocket
import json
import sys


async def test_invalid_json():
    print("\n=== Тест 1: Невалидный JSON ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            await ws.send_message("Это вообще не JSON")
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_missing_busid():
    print("\n=== Тест 2: Отсутствие busId ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "lat": 55.75,
                "lng": 37.62,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_missing_lat():
    print("\n=== Тест 3: Отсутствие lat ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lng": 37.62,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_missing_lng():
    print("\n=== Тест 4: Отсутствие lng ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 55.75,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_missing_route():
    print("\n=== Тест 5: Отсутствие route ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 55.75,
                "lng": 37.62
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_busid_not_string():
    print("\n=== Тест 6: busId не строка ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": 12345,
                "lat": 55.75,
                "lng": 37.62,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_lat_not_number():
    print("\n=== Тест 7: lat не число ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": "не число",
                "lng": 37.62,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_lng_not_number():
    print("\n=== Тест 8: lng не число ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 55.75,
                "lng": "не число",
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_route_not_string():
    print("\n=== Тест 9: route не строка ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 55.75,
                "lng": 37.62,
                "route": 156
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_lat_out_of_range_high():
    print("\n=== Тест 10: lat > 90 ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 150,
                "lng": 37.62,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_lat_out_of_range_low():
    print("\n=== Тест 11: lat < -90 ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": -100,
                "lng": 37.62,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_lng_out_of_range_high():
    print("\n=== Тест 12: lng > 180 ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 55.75,
                "lng": 200,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_lng_out_of_range_low():
    print("\n=== Тест 13: lng < -180 ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 55.75,
                "lng": -200,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_empty_message():
    print("\n=== Тест 14: Пустое сообщение ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            await ws.send_message("")
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_null_values():
    print("\n=== Тест 15: null значения ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": None,
                "lat": None,
                "lng": None,
                "route": None
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_extra_fields():
    print("\n=== Тест 16: Лишние поля (должны игнорироваться) ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 55.75,
                "lng": 37.62,
                "route": "156",
                "extra_field": "лишнее поле",
                "hack": "попытка взлома"
            })
            await ws.send_message(message)
            print("Лишние поля отправлены, ошибок нет (они игнорируются)")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_valid_data():
    print("\n=== Тест 17: Корректные данные ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "valid-bus-001",
                "lat": 55.7558,
                "lng": 37.6173,
                "route": "156"
            })
            await ws.send_message(message)
            print("Корректные данные отправлены, ошибок нет")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_sql_injection():
    print("\n=== Тест 18: Попытка SQL инъекции ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "'; DROP TABLE buses; --",
                "lat": 55.75,
                "lng": 37.62,
                "route": "156' OR '1'='1"
            })
            await ws.send_message(message)
            print("SQL инъекция отправлена, сервер должен экранировать или отклонить")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_xss_attack():
    print("\n=== Тест 19: Попытка XSS атаки ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "<script>alert('XSS')</script>",
                "lat": 55.75,
                "lng": 37.62,
                "route": "<img src=x onerror=alert('XSS')>"
            })
            await ws.send_message(message)
            print("XSS атака отправлена, сервер должен экранировать")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_unicode_overflow():
    print("\n=== Тест 20: Очень длинные строки (потенциальный DoS) ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            long_string = "A" * 100000  # 100KB строка
            message = json.dumps({
                "busId": long_string,
                "lat": 55.75,
                "lng": 37.62,
                "route": long_string[:100]
            })
            await ws.send_message(message)
            print("Длинные строки отправлены, сервер должен обработать")
    except Exception as e:
        print(f"Ошибка: {e}")


async def main():
    print("=" * 70)
    print(" ТЕСТИРОВАНИЕ ВАЛИДАЦИИ АВТОБУСОВ НА СЕРВЕРЕ")
    print("=" * 70)
    print("\n  Убедитесь что сервер запущен!")
    print("   Запустите сервер: python server.py -v")
    print("\n Тестирование различных вредоносных сценариев...\n")
    
    input("Нажмите Enter для начала тестов...")
    
    tests = [
        ("Невалидный JSON", test_invalid_json),
        ("Отсутствие busId", test_missing_busid),
        ("Отсутствие lat", test_missing_lat),
        ("Отсутствие lng", test_missing_lng),
        ("Отсутствие route", test_missing_route),
        ("busId не строка", test_busid_not_string),
        ("lat не число", test_lat_not_number),
        ("lng не число", test_lng_not_number),
        ("route не строка", test_route_not_string),
        ("lat > 90", test_lat_out_of_range_high),
        ("lat < -90", test_lat_out_of_range_low),
        ("lng > 180", test_lng_out_of_range_high),
        ("lng < -180", test_lng_out_of_range_low),
        ("Пустое сообщение", test_empty_message),
        ("null значения", test_null_values),
        ("Лишние поля", test_extra_fields),
        ("Корректные данные", test_valid_data),
        ("SQL инъекция", test_sql_injection),
        ("XSS атака", test_xss_attack),
        ("Очень длинные строки", test_unicode_overflow),
    ]
    
    for name, test_func in tests:
        print(f"\n{'─' * 60}")
        print(f" {name}")
        print('─' * 60)
        await test_func()
        await trio.sleep(0.5)
    
    print("\n" + "=" * 70)
    print(" ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ")
    print("=" * 70)
    print("\n Ожидаемые результаты:")
    print("   • Невалидный JSON → ошибка")
    print("   • Отсутствие полей → ошибка")
    print("   • Неправильные типы → ошибка")
    print("   • Координаты вне диапазона → ошибка")
    print("   • Корректные данные → успех")
    print("   • SQL/XSS инъекции → должны быть экранированы или отклонены")


if __name__ == '__main__':
    try:
        trio.run(main)
    except KeyboardInterrupt:
        print("\n Тесты остановлены")
    except ConnectionRefusedError:
        print("\n Сервер не запущен!")
        print("   Запустите в другом терминале: python server.py -v")
        sys.exit(1)
