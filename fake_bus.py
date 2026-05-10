import trio
import trio_websocket
import json
import random
import sys
import argparse
import logging
import time
import functools
from pathlib import Path
from itertools import cycle, islice
from typing import Dict, Any, Callable


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def relaunch_on_disconnect(async_function: Callable):
    @functools.wraps(async_function)
    async def wrapper(*args, **kwargs):
        reconnect_delay = 1.0
        max_reconnect_delay = 30.0
        attempt = 0
        
        while True:
            try:
                attempt += 1
                logger.info(f"Попытка подключения #{attempt}...")
                
                await async_function(*args, **kwargs)
                
                logger.info("Функция завершилась нормально")
                break
                
            except (trio_websocket.ConnectionClosed,
                    trio_websocket.HandshakeError,
                    ConnectionRefusedError,
                    OSError) as e:
                logger.warning(f"Соединение разорвано: {type(e).__name__}")

                delay = min(reconnect_delay * (1.5 ** attempt), max_reconnect_delay)
                jitter = random.uniform(0.8, 1.2)
                actual_delay = delay * jitter
                
                logger.info(f"Повторная попытка через {actual_delay:.1f} секунд...")
                await trio.sleep(actual_delay)
                
            except KeyboardInterrupt:
                logger.info("Остановлено пользователем")
                raise
                
            except Exception as e:
                logger.error(f"Неожиданная ошибка: {type(e).__name__}: {e}")
                logger.info("Попытка переподключения через 5 секунд...")
                await trio.sleep(5.0)
                
    return wrapper

def setup_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Имитатор автобусов с автоматическим переподключением',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s --server ws://localhost:8080 --routes 50 --buses-per-route 10
  %(prog)s --routes-number 20 --buses-per-route 100 --websockets-number 5
        """
    )
    
    parser.add_argument('-s', '--server', default='ws://127.0.0.1:8080',
                       help='Адрес WebSocket сервера')
    parser.add_argument('-r', '--routes-number', type=int, default=50,
                       help='Количество используемых маршрутов')
    parser.add_argument('-b', '--buses-per-route', type=int, default=10,
                       help='Количество автобусов на каждом маршруте')
    parser.add_argument('-w', '--websockets-number', type=int, default=10,
                       help='Количество открытых WebSocket соединений')
    parser.add_argument('-e', '--emulator-id', default='',
                       help='Префикс к busId')
    parser.add_argument('-t', '--refresh-timeout', type=float, default=1.0,
                       help='Задержка обновления координат (секунды)')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                       help='Уровень детализации логирования')
    parser.add_argument('--reconnect-delay', type=float, default=1.0,
                       help='Базовая задержка переподключения (секунды)')
    parser.add_argument('--max-reconnect-delay', type=float, default=30.0,
                       help='Максимальная задержка переподключения')
    
    return parser.parse_args()

def configure_logging(verbose_level: int):
    if verbose_level == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif verbose_level >= 2:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.WARNING)

def load_routes(routes_dir: str) -> Dict[str, Dict[str, Any]]:
    routes = {}
    routes_path = Path(routes_dir)
    
    if not routes_path.exists():
        logger.error(f"Директория не найдена: {routes_dir}")
        return None
    
    for file_path in routes_path.glob('*.json'):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            route_name = data.get('name', file_path.stem)
            routes[route_name] = {
                'name': route_name,
                'coordinates': data['coordinates']
            }
        except Exception:
            continue
    
    logger.info(f"Загружено {len(routes)} маршрутов")
    return routes

def generate_bus_id(route_id: str, bus_index: int, emulator_id: str = '') -> str:
    if emulator_id:
        return f"{route_id}-{bus_index:04d}-{emulator_id}"
    return f"{route_id}-{bus_index:04d}"

async def run_bus(bus_id: str, route_data: Dict[str, Any], 
                  send_channel: trio.MemorySendChannel, refresh_timeout: float):
    route_name = route_data['name']
    coordinates = route_data['coordinates']
    
    start_offset = random.randint(0, len(coordinates) - 1)
    coordinates_cycle = cycle(coordinates)
    coordinates_iter = islice(coordinates_cycle, start_offset, None)
    
    logger.debug(f"Автобус {bus_id} запущен (начало: {start_offset})")
    
    try:
        while True:
            lat, lng = next(coordinates_iter)
            
            bus_data = {
                "busId": bus_id,
                "lat": lat,
                "lng": lng,
                "route": route_name
            }
            
            try:
                await send_channel.send(bus_data)
            except (trio.BrokenResourceError, trio.ClosedResourceError):
                logger.debug(f"Автобус {bus_id}: канал закрыт")
                break
            
            await trio.sleep(refresh_timeout)
            
    except (trio.BrokenResourceError, trio.ClosedResourceError):
        pass
    except Exception as e:
        logger.debug(f"Автобус {bus_id}: {type(e).__name__}")

@relaunch_on_disconnect
async def send_updates(server_address: str, receive_channel: trio.MemoryReceiveChannel, 
                       channel_id: int, batch_size: int = 100):
    logger.info(f"Канал {channel_id}: подключение к {server_address}")
    
    async with trio_websocket.open_websocket_url(server_address) as ws:
        logger.info(f"Канал {channel_id}: подключен успешно")
        
        batch = []
        total_sent = 0
        last_log_time = time.time()
        connection_start = time.time()
        
        async for bus_data in receive_channel:
            batch.append(bus_data)
            
            if len(batch) >= batch_size:
                for data in batch:
                    await ws.send_message(json.dumps(data, ensure_ascii=False))
                
                total_sent += len(batch)
                batch.clear()
                
                current_time = time.time()
                if current_time - last_log_time > 5:
                    elapsed = current_time - connection_start
                    rate = total_sent / elapsed if elapsed > 0 else 0
                    logger.info(f"Канал {channel_id}: отправлено {total_sent} сообщений "
                              f"({rate:.1f}/сек)")
                    last_log_time = current_time
        
        if batch:
            for data in batch:
                await ws.send_message(json.dumps(data, ensure_ascii=False))
            logger.info(f"Канал {channel_id}: отправлены оставшиеся {len(batch)} сообщений")

async def launch_simulation(args: argparse.Namespace):
    logger.info("=" * 80)
    logger.info("ИМИТАТОР АВТОБУСОВ ")
    logger.info("=" * 80)
    
    routes = load_routes('routes')
    if not routes:
        logger.error("Не удалось загрузить маршруты")
        return
    
    available_routes = min(args.routes_number, len(routes))
    selected_routes = list(routes.items())[:available_routes]
    
    total_buses = available_routes * args.buses_per_route
    logger.info(f"Будут запущены {total_buses} автобусов на {available_routes} маршрутах")
    logger.info(f"Используется {args.websockets_number} WebSocket соединений")
    logger.info("=" * 80)
    
    send_channels = []
    receive_channels = []
    
    for i in range(args.websockets_number):
        send_chan, receive_chan = trio.open_memory_channel(max_buffer_size=10000)
        send_channels.append(send_chan)
        receive_channels.append(receive_chan)
    
    try:
        async with trio.open_nursery() as nursery:
            for i, receive_chan in enumerate(receive_channels):
                nursery.start_soon(
                    send_updates,
                    args.server,
                    receive_chan,
                    i + 1,
                    100
                )
            
            logger.info(f" Запущено {args.websockets_number} отправителей")
            logger.info(" Ожидание подключения к серверу...")

            await trio.sleep(2.0)
            
            logger.info("Запуск автобусов...")
            
            buses_launched = 0
            for route_name, route_data in selected_routes:
                for i in range(args.buses_per_route):
                    bus_id = generate_bus_id(route_name, i, args.emulator_id)
                    channel_idx = buses_launched % args.websockets_number
                    
                    nursery.start_soon(
                        run_bus,
                        bus_id,
                        route_data,
                        send_channels[channel_idx],
                        args.refresh_timeout
                    )
                    
                    buses_launched += 1
                    
                    if buses_launched % 1000 == 0:
                        logger.info(f" Запущено {buses_launched}/{total_buses} автобусов")
            
            logger.info(f"Все {buses_launched} автобусов запущены!")
            logger.info("\n" + "=" * 80)
            logger.info("СИСТЕМА РАБОТАЕТ В НОРМАЛЬНОМ РЕЖИМЕ")
            logger.info("=" * 80)
            logger.info("Особенности:")
            logger.info("   • Автоматическое переподключение при разрыве связи")
            logger.info("   • Экспоненциальная задержка между попытками")
            logger.info("   • Устойчивость к перезапускам сервера")
            logger.info("=" * 80)
            logger.info("\n Ctrl+C для остановки")
            
            await trio.sleep_forever()
            
    except KeyboardInterrupt:
        logger.info("\n Получен сигнал остановки")

async def main():
    args = setup_cli()
    configure_logging(args.verbose)
    
    logger.info("Имитатор запущен")
    logger.info(f"Сервер: {args.server}")
    
    try:
        await launch_simulation(args)
    except KeyboardInterrupt:
        logger.info("\n Корректное завершение работы")
    except Exception as e:
        logger.error(f" Ошибка: {type(e).__name__}: {e}")
        if logger.isEnabledFor(logging.DEBUG):
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    try:
        trio.run(main, strict_exception_groups=False)
    except KeyboardInterrupt:
        print("\n Имитатор остановлен")
    except Exception as e:
        print(f"\n Критическая ошибка: {e}")
        sys.exit(1)
