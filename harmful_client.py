import trio
import trio_websocket
import json
import sys


async def test_invalid_json():
    print("\n=== Тест 1: Невалидный JSON ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            await ws.send_message("Это не JSON")
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_missing_msgtype():
    print("\n=== Тест 2: Отсутствие msgType ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({"data": "test"})
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_invalid_bounds():
    print("\n=== Тест 3: Некорректные границы (юг > север) ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "msgType": "newBounds",
                "data": {
                    "south_lat": 60,
                    "north_lat": 55,
                    "west_lng": 30,
                    "east_lng": 40
                }
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_missing_bounds_fields():
    print("\n=== Тест 4: Отсутствие полей в границах ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "msgType": "newBounds",
                "data": {
                    "south_lat": 55
                }
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_invalid_bus_data():
    print("\n=== Тест 5: Некорректные данные автобуса ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": 123,
                "lat": "не число",
                "lng": 37.62,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_missing_bus_fields():
    print("\n=== Тест 6: Отсутствие полей автобуса ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 55.75
                # Нет lng и route
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_out_of_range_coords():
    print("\n=== Тест 7: Координаты вне диапазона ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "test-bus",
                "lat": 200,
                "lng": 400,
                "route": "156"
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_unknown_msgtype():
    print("\n=== Тест 8: Неизвестный тип сообщения ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "msgType": "UnknownType",
                "data": {}
            })
            await ws.send_message(message)
            response = await ws.get_message()
            print(f"Response: {response}")
    except Exception as e:
        print(f"Ошибка: {e}")


async def test_valid_bus_data():
    print("\n=== Тест 9: Корректные данные (без ошибок) ===")
    try:
        async with trio_websocket.open_websocket_url('ws://127.0.0.1:8080') as ws:
            message = json.dumps({
                "busId": "valid-bus",
                "lat": 55.75,
                "lng": 37.62,
                "route": "156"
            })
            await ws.send_message(message)
            print("Корректные данные отправлены, ошибок нет")
    except Exception as e:
        print(f"Ошибка: {e}")


async def main():
    print("=" * 60)
    print(" ТЕСТИРОВАНИЕ ВАЛИДАЦИИ СЕРВЕРА")
    print("=" * 60)
    print("\n  Убедитесь что сервер запущен!")
    print("   Запустите сервер: python server.py\n")
    
    input("Нажмите Enter для начала тестов...")
    
    await test_invalid_json()
    await test_missing_msgtype()
    await test_invalid_bounds()
    await test_missing_bounds_fields()
    await test_invalid_bus_data()
    await test_missing_bus_fields()
    await test_out_of_range_coords()
    await test_unknown_msgtype()
    await test_valid_bus_data()
    
    print("\n" + "=" * 60)
    print(" ТЕСТЫ ЗАВЕРШЕНЫ")
    print("=" * 60)


if __name__ == '__main__':
    try:
        trio.run(main)
    except KeyboardInterrupt:
        print("\n Тесты остановлены")
    except ConnectionRefusedError:
        print("\n Сервер не запущен! Запустите python server.py")
        sys.exit(1)
