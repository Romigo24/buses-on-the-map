import trio
import trio_websocket
import json
import logging
import sys
import time
import argparse
from dataclasses import dataclass
from typing import Dict, List, Any, Tuple
from contextlib import suppress


logging.basicConfig(
    level=logging.INFO,
    format='%(name)s:%(message)s',
    datefmt=''
)

logger = logging.getLogger('server')

logging.getLogger('trio-websocket').setLevel(logging.WARNING)
logging.getLogger('trio').setLevel(logging.WARNING)


def setup_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Сервер автобусов для передачи данных в браузер',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '-b', '--bus-port',
        type=int,
        default=8080,
        help='Порт для подключения имитаторов автобусов (по умолчанию: 8080)'
    )
    
    parser.add_argument(
        '-r', '--browser-port',
        type=int,
        default=8000,
        help='Порт для подключения браузеров (по умолчанию: 8000)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Уровень детализации логирования (-v: INFO, -vv: DEBUG)'
    )
    
    return parser.parse_args()


def configure_logging(verbose_level: int):
    if verbose_level == 1:
        logger.setLevel(logging.INFO)
        logging.getLogger().setLevel(logging.INFO)
    elif verbose_level >= 2:
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Режим отладки включен")
    else:
        logger.setLevel(logging.WARNING)
        logging.getLogger().setLevel(logging.WARNING)


def create_error_response(errors: List[str]) -> str:
    """Создает JSON ответ с ошибками"""
    return json.dumps({
        "msgType": "Errors",
        "errors": errors
    }, ensure_ascii=False)


def validate_bus_data(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors = []

    required_fields = ['busId', 'lat', 'lng', 'route']
    for field in required_fields:
        if field not in data:
            errors.append(f"Missing required field: {field}")

    if errors:
        return False, errors

    if not isinstance(data['busId'], str):
        errors.append("busId must be a string")

    try:
        lat = float(data['lat'])
        if not (-90 <= lat <= 90):
            errors.append(f"lat must be between -90 and 90, got {lat}")
    except (ValueError, TypeError):
        errors.append(f"lat must be a number, got {data['lat']}")

    try:
        lng = float(data['lng'])
        if not (-180 <= lng <= 180):
            errors.append(f"lng must be between -180 and 180, got {lng}")
    except (ValueError, TypeError):
        errors.append(f"lng must be a number, got {data['lng']}")

    if not isinstance(data['route'], str):
        errors.append("route must be a string")

    return len(errors) == 0, errors


def validate_bounds_data(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors = []

    required_fields = ['south_lat', 'north_lat', 'west_lng', 'east_lng']
    for field in required_fields:
        if field not in data:
            errors.append(f"Missing required field: {field}")

    if errors:
        return False, errors

    try:
        south_lat = float(data['south_lat'])
        north_lat = float(data['north_lat'])
        west_lng = float(data['west_lng'])
        east_lng = float(data['east_lng'])

        if not (-90 <= south_lat <= 90):
            errors.append(f"south_lat must be between -90 and 90, got {south_lat}")
        if not (-90 <= north_lat <= 90):
            errors.append(f"north_lat must be between -90 and 90, got {north_lat}")
        if not (-180 <= west_lng <= 180):
            errors.append(f"west_lng must be between -180 and 180, got {west_lng}")
        if not (-180 <= east_lng <= 180):
            errors.append(f"east_lng must be between -180 and 180, got {east_lng}")

        if south_lat >= north_lat:
            errors.append(f"south_lat ({south_lat}) must be less than north_lat ({north_lat})")
        if west_lng >= east_lng:
            errors.append(f"west_lng ({west_lng}) must be less than east_lng ({east_lng})")
    except (ValueError, TypeError) as e:
        errors.append(f"Invalid numeric values: {e}")

    return len(errors) == 0, errors


@dataclass
class Bus:
    bus_id: str
    lat: float
    lng: float
    route: str
    timestamp: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "busId": self.bus_id,
            "lat": self.lat,
            "lng": self.lng,
            "route": self.route
        }


@dataclass
class WindowBounds:
    south_lat: float
    north_lat: float
    west_lng: float
    east_lng: float
    
    def is_inside(self, lat: float, lng: float) -> bool:
        return (self.south_lat <= lat <= self.north_lat and
                self.west_lng <= lng <= self.east_lng)
    
    def update(self, south_lat: float, north_lat: float, 
               west_lng: float, east_lng: float) -> None:
        self.south_lat = south_lat
        self.north_lat = north_lat
        self.west_lng = west_lng
        self.east_lng = east_lng
    
    @classmethod
    def from_dict(cls, data: Dict[str, float]) -> 'WindowBounds':
        return cls(
            south_lat=data.get('south_lat', 0),
            north_lat=data.get('north_lat', 0),
            west_lng=data.get('west_lng', 0),
            east_lng=data.get('east_lng', 0)
        )


class BusServer:
    def __init__(self, bus_port: int, browser_port: int):
        self.bus_port = bus_port
        self.browser_port = browser_port
        
        self.buses: Dict[str, Bus] = {}
        self._lock = trio.Lock()
        
        self.browsers: Dict[trio_websocket.WebSocketConnection, WindowBounds] = {}
        
        logger.info(f"Сервер инициализирован (bus_port={bus_port}, browser_port={browser_port})")
    
    async def send_error(self, ws, errors: List[str]):
        try:
            await ws.send_message(create_error_response(errors))
            logger.warning(f"Отправлена ошибка клиенту: {errors}")
        except Exception as e:
            logger.error(f"Не удалось отправить ошибку: {e}")

    async def send_buses_to_browser(self, ws, bounds: WindowBounds):
        async with self._lock:
            visible_buses = [
                bus for bus in self.buses.values()
                if bounds.is_inside(bus.lat, bus.lng)
            ]

        message = {
            "msgType": "Buses",
            "buses": [bus.to_dict() for bus in visible_buses]
        }

        try:
            await ws.send_message(json.dumps(message, ensure_ascii=False))
            if visible_buses and logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Отправлено {len(visible_buses)} автобусов в браузер")
        except trio_websocket.ConnectionClosed:
            pass
    
    async def talk_to_browser(self, ws: trio_websocket.WebSocketConnection, 
                               bounds: WindowBounds):
        browser_id = id(ws)
        logger.debug(f"Начат фоновый таск talk_to_browser для браузера #{browser_id}")
        
        try:
            while True:
                await self.send_buses_to_browser(ws, bounds)
                await trio.sleep(1.0)
        except Exception as e:
            logger.error(f"Ошибка в talk_to_browser: {e}")
    
    async def listen_browser(self, ws: trio_websocket.WebSocketConnection, 
                             bounds: WindowBounds):
        browser_id = id(ws)
        logger.debug(f"Начато прослушивание браузера #{browser_id}")
        
        try:
            while True:
                try:
                    message = await ws.get_message()

                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError as e:
                        await self.send_error(ws, [f"Invalid JSON: {e}"])
                        continue

                    if 'msgType' not in data:
                        await self.send_error(ws, ["Requires msgType specified"])
                        continue
                    
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(json.dumps(data))
                    
                    if data.get('msgType') == 'newBounds':
                        bounds_data = data.get('data', {})

                        is_valid, errors = validate_bounds_data(bounds_data)
                        if not is_valid:
                            await self.send_error(ws, errors)
                            continue

                        bounds.update(
                            south_lat=float(bounds_data.get('south_lat', 0)),
                            north_lat=float(bounds_data.get('north_lat', 0)),
                            west_lng=float(bounds_data.get('west_lng', 0)),
                            east_lng=float(bounds_data.get('east_lng', 0))
                        )
                        
                        logger.debug("Границы обновлены")
                    else:
                        await self.send_error(ws, [f"Unknown msgType: {data.get('msgType')}"])
                    
                except json.JSONDecodeError:
                    await self.send_error(ws, ["Requires valid JSON"])
                except trio_websocket.ConnectionClosed:
                    logger.info(f"Браузер #{browser_id} закрыл соединение")
                    break
                    
        except Exception as e:
            logger.error(f"Ошибка в listen_browser: {e}")
        finally:
            logger.debug(f"Завершено прослушивание браузера #{browser_id}")
    
    async def handle_browser_connection(self, request):
        ws = await request.accept()
        logger.info("Новый браузер подключился")
        
        initial_bounds = WindowBounds(
            south_lat=55.5,
            north_lat=55.9,
            west_lng=37.3,
            east_lng=37.9
        )
        
        async with self._lock:
            self.browsers[ws] = initial_bounds

        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.listen_browser, ws, initial_bounds)
            nursery.start_soon(self.talk_to_browser, ws, initial_bounds)
        
        async with self._lock:
            self.browsers.pop(ws, None)
        logger.info("Работа с браузером завершена")
    
    async def handle_bus_connection(self, request):
        ws = await request.accept()
        logger.info("Имитатор автобусов подключился")
        
        try:
            while True:
                try:
                    message = await ws.get_message()
                    
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError as e:
                        await self.send_error(ws, [f"Invalid JSON: {e}"])
                        continue
                    
                    is_valid, errors = validate_bus_data(data)
                    if not is_valid:
                        await self.send_error(ws, errors)
                        continue

                    bus = Bus(
                        bus_id=data['busId'],
                        lat=float(data['lat']),
                        lng=float(data['lng']),
                        route=data['route'],
                        timestamp=time.time()
                    )
                    
                    async with self._lock:
                        self.buses[bus.bus_id] = bus
                    
                except trio_websocket.ConnectionClosed:
                    logger.info("Имитатор автобусов отключился")
                    break
                    
        except Exception as e:
            logger.error(f"Ошибка: {e}")
    
    async def cleanup_old_buses(self):
        while True:
            await trio.sleep(30.0)
            current_time = time.time()
            async with self._lock:
                old_buses = [
                    bus_id for bus_id, bus in self.buses.items()
                    if current_time - bus.timestamp > 30.0
                ]
                for bus_id in old_buses:
                    del self.buses[bus_id]
                if old_buses and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Удалено {len(old_buses)} устаревших автобусов")
    
    async def run(self):
        logger.info("=" * 60)
        logger.info(" СЕРВЕР АВТОБУСОВ (с валидацией)")
        logger.info("=" * 60)
        logger.info(f" Браузеры: ws://127.0.0.1:{self.browser_port}")
        logger.info(f" Автобусы: ws://127.0.0.1:{self.bus_port}")
        logger.info("=" * 60)
        
        async with trio.open_nursery() as nursery:
            async def run_browser_server():
                await trio_websocket.serve_websocket(
                    self.handle_browser_connection,
                    '127.0.0.1',
                    self.browser_port,
                    ssl_context=None
                )
            
            async def run_bus_server():
                await trio_websocket.serve_websocket(
                    self.handle_bus_connection,
                    '127.0.0.1',
                    self.bus_port,
                    ssl_context=None
                )
            
            nursery.start_soon(run_browser_server)
            nursery.start_soon(run_bus_server)
            nursery.start_soon(self.cleanup_old_buses)
            
            logger.info(" Серверы запущены. Ожидание подключений...")
            await trio.sleep_forever()


async def main():
    args = setup_cli()
    configure_logging(args.verbose)
    
    logger.info("Запуск сервера с параметрами:")
    logger.info(f"  bus_port={args.bus_port}, browser_port={args.browser_port}")
    
    server = BusServer(bus_port=args.bus_port, browser_port=args.browser_port)
    await server.run()


if __name__ == '__main__':
    with suppress(KeyboardInterrupt):
        trio.run(main)
    
    print("\n Сервер завершил работу")
    sys.exit(0)
