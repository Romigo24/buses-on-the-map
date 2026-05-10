import trio
import trio_websocket
import json
import logging
import sys
import time
import argparse
from typing import Dict, List, Any
from contextlib import suppress
from pydantic import BaseModel, Field, ValidationError, validator
from pydantic import ConfigDict


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


class BusData(BaseModel):
    model_config = ConfigDict(extra='forbid')

    busId: str = Field(..., min_length=1)
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)
    route: str = Field(..., min_length=1)

    @validator('busId')
    def bus_id_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('busId cannot be empty')
        return v.strip()

    @validator('route')
    def route_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('route cannot be empty')
        return v.strip()


class WindowBoundsData(BaseModel):
    model_config = ConfigDict(extra='forbid')

    south_lat: float = Field(..., ge=-90, le=90)
    north_lat: float = Field(..., ge=-90, le=90)
    west_lng: float = Field(..., ge=-180, le=180)
    east_lng: float = Field(..., ge=-180, le=180)

    @validator('north_lat')
    def north_greater_than_south(cls, v, values):
        if 'south_lat' in values and v <= values['south_lat']:
            raise ValueError('north_lat must be greater than south_lat')
        return v

    @validator('east_lng')
    def east_greater_than_west(cls, v, values):
        if 'west_lng' in values and v <= values['west_lng']:
            raise ValueError('east_lng must be greater than west_lng')
        return v


class NewBoundsMessage(BaseModel):
    model_config = ConfigDict(extra='forbid')

    msgType: str
    data: WindowBoundsData

    @validator('msgType')
    def validate_msg_type(cls, v):
        if v != 'newBounds':
            raise ValueError(f'Unknown msgType: {v}')
        return v


def create_error_response(errors: List[str]) -> str:
    return json.dumps({
        "msgType": "Errors",
        "errors": errors
    }, ensure_ascii=False)


class Bus:
    def __init__(self, bus_id: str, lat: float, lng: float, route: str, timestamp: float):
        self.bus_id = bus_id
        self.lat = lat
        self.lng = lng
        self.route = route
        self.timestamp = timestamp
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "busId": self.bus_id,
            "lat": self.lat,
            "lng": self.lng,
            "route": self.route
        }


class WindowBounds:
    def __init__(self, south_lat: float, north_lat: float, west_lng: float, east_lng: float):
        self.south_lat = south_lat
        self.north_lat = north_lat
        self.west_lng = west_lng
        self.east_lng = east_lng
    
    def is_inside(self, lat: float, lng: float) -> bool:
        return (self.south_lat <= lat <= self.north_lat and
                self.west_lng <= lng <= self.east_lng)
    
    def update(self, south_lat: float, north_lat: float, 
               west_lng: float, east_lng: float) -> None:
        self.south_lat = south_lat
        self.north_lat = north_lat
        self.west_lng = west_lng
        self.east_lng = east_lng


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

                    try:
                        bounds_message = NewBoundsMessage(**data)

                        bounds.update(
                            south_lat=bounds_message.data.south_lat,
                            north_lat=bounds_message.data.north_lat,
                            west_lng=bounds_message.data.west_lng,
                            east_lng=bounds_message.data.east_lng
                        )
                        
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug("Границы обновлены")

                    except ValidationError as e:
                        errors = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
                        await self.send_error(ws, errors)
                        continue
                    
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
                    
                    try:
                        bus_data = BusData(**data)

                        bus = Bus(
                            bus_id=bus_data.busId,
                            lat=bus_data.lat,
                            lng=bus_data.lng,
                            route=bus_data.route,
                            timestamp=time.time()
                        )

                        async with self._lock:
                            self.buses[bus.bus_id] = bus

                    except ValidationError as e:
                        errors = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
                        await self.send_error(ws, errors)
                        continue
                    
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
        logger.info(" СЕРВЕР АВТОБУСОВ ")
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