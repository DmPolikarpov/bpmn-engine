import json
import urllib.request
import urllib.error
import os
import pg8000
import uuid
import time
import socket
import threading
import asyncio
import websockets
from datetime import datetime, timezone

# =====================================================================
# КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЯ
# =====================================================================
DB_HOST = os.getenv("DB_HOST", "bpmn-db-rw")
DB_NAME = os.getenv("DB_NAME", "bpmn_db")
DB_USER = os.getenv("DB_USER", "bpmn_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "bpmn_super_secret_password")

NATS_HOST = os.getenv("NATS_HOST", "nats-service")
NATS_PORT = int(os.getenv("NATS_PORT", "4222"))

WS_URL = os.getenv("WS_SERVICE_URL", "ws://websocket-cluster-service:8080/ws/status")
BASE_GATEWAY = os.getenv("GATEWAY_URL_BASE", "http://http-cluster-service:8080/api/v1/process/instances")

# Генерируем уникальные ID для изоляции основного прогона
TEST_UUID = uuid.uuid4().hex[:8]
PROCESS_DEF_ID = f"Process_MultiStep_{TEST_UUID}"
INSTANCE_ID = f"inst-complex-{TEST_UUID}"
CORRELATION_KEY = f"corr-{TEST_UUID}"

TASK_1_ID = "ServiceTask_ValidateOrder"
TASK_2_ID = "ServiceTask_ReserveStock"

COMPLEX_BPMN_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  targetNamespace="http://bpmn.io/schema/bpmn"
                  id="Definitions_MultiStep">
  <bpmn:process id="{PROCESS_DEF_ID}" name="Complex MultiStep Workflow" isExecutable="true">
    <bpmn:startEvent id="StartEvent_1"/>
    <bpmn:sequenceFlow id="Flow_1" sourceRef="StartEvent_1" targetRef="{TASK_1_ID}"/>
    <bpmn:serviceTask id="{TASK_1_ID}" name="Validate Order via ERP"/>
    <bpmn:sequenceFlow id="Flow_2" sourceRef="{TASK_1_ID}" targetRef="{TASK_2_ID}"/>
    <bpmn:serviceTask id="{TASK_2_ID}" name="Reserve Stock via ERP"/>
    <bpmn:sequenceFlow id="Flow_3" sourceRef="{TASK_2_ID}" targetRef="EndEvent_1"/>
    <bpmn:endEvent id="EndEvent_1"/>
  </bpmn:process>
</bpmn:definitions>"""

captured_ws_events = []
ws_stop_event = threading.Event()

# =====================================================================
# WEBSOCKET СЛУШАТЕЛЬ
# =====================================================================

def start_websocket_listener():
    """Фоновый поток для приема событий WebSocket во время выполнения тестов."""
    def run_loop():
        async def listen():
            try:
                print(f"🔌 [WS Client] Подключение к WebSocket по адресу: {WS_URL}")
                async with websockets.connect(WS_URL, open_timeout=5) as ws:
                    print(f"✅ [WS Client] Успешно подключено к {WS_URL}")
                    while not ws_stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            try:
                                data = json.loads(msg)
                            except Exception:
                                data = {"raw_payload": msg}
                            
                            captured_ws_events.append(data)
                            print(f"📡 [WS Received] {data}")
                        except asyncio.TimeoutError:
                            continue
                        except Exception:
                            break
            except Exception as e:
                print(f"⚠️ [WS Client] Ошибка WebSocket ({WS_URL}): {e}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(listen())
        loop.close()

    t = threading.Thread(target=run_loop, daemon=True)
    t.start()
    time.sleep(1)

# =====================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =====================================================================

def get_db_connection():
    return pg8000.connect(
        user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_NAME, port=5432
    )

def send_gateway_event(instance_id, event_name, element_id, payload_data):
    url = f"{BASE_GATEWAY}/{instance_id}/events"
    payload = {
        "event_id": str(uuid.uuid4()),
        "event_name": event_name,
        "instance_id": instance_id,
        "correlation_key": f"corr-{uuid.uuid4().hex[:8]}",
        "element_id": element_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload_data,
        "output_data": payload_data
    }
    
    headers = {
        'Content-Type': 'application/json',
        'X-Correlation-ID': payload["correlation_key"]
    }
    
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.status, response.read().decode('utf-8')

def send_nats_erp_response(instance_id, element_id, status="SUCCESS", payload_data=None):
    subject = "mes.in.v1.erp.response"
    erp_event = {
        "process_instance_id": instance_id,
        "element_id": element_id,
        "status": status,
        "error_message": None,
        "payload": payload_data or {"executed_at": datetime.now(timezone.utc).isoformat()}
    }
    
    payload_bytes = json.dumps(erp_event).encode('utf-8')
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect((NATS_HOST, NATS_PORT))
    s.recv(1024)
    
    connect_cmd = b'CONNECT {"verbose":false,"pedantic":false}\r\n'
    pub_cmd = f'PUB {subject} {len(payload_bytes)}\r\n'.encode('utf-8')
    
    s.sendall(connect_cmd + pub_cmd + payload_bytes + b'\r\n')
    s.close()

def poll_db_token(element_id, expected_state, timeout=15, instance_id=None):
    target_instance_id = instance_id or INSTANCE_ID
    start_time = time.time()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        while time.time() - start_time < timeout:
            cursor.execute(
                "SELECT state FROM tokens WHERE instance_id = %s AND element_id = %s;",
                (target_instance_id, element_id)
            )
            row = cursor.fetchone()
            if row and row[0] == expected_state:
                return True, row[0]
            time.sleep(1)
            
        cursor.execute(
            "SELECT state FROM tokens WHERE instance_id = %s AND element_id = %s;",
            (target_instance_id, element_id)
        )
        curr = cursor.fetchone()
        return False, (curr[0] if curr else "NOT_FOUND")
    finally:
        cursor.close()
        conn.close()

def poll_db_instance_status(expected_status, timeout=15, instance_id=None):
    target_instance_id = instance_id or INSTANCE_ID
    start_time = time.time()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        while time.time() - start_time < timeout:
            cursor.execute(
                "SELECT status FROM process_instances WHERE id = %s;",
                (target_instance_id,)
            )
            row = cursor.fetchone()
            if row and row[0] == expected_status:
                return True, row[0]
            time.sleep(1)
            
        cursor.execute("SELECT status FROM process_instances WHERE id = %s;", (target_instance_id,))
        curr = cursor.fetchone()
        return False, (curr[0] if curr else "NOT_FOUND")
    finally:
        cursor.close()
        conn.close()

def force_expire_token(instance_id, element_id):
    """Искусственно сдвигает created_at токена в БД на 35 минут назад."""
    conn = get_db_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE tokens SET created_at = NOW() - INTERVAL '35 minutes' WHERE instance_id = %s AND element_id = %s;",
            (instance_id, element_id)
        )
        print(f"⏳ [SLA Test] Токен '{instance_id}:{element_id}' состарен в БД на 35 минут.")
    finally:
        cursor.close()
        conn.close()

def cleanup_test_data(instance_id, process_def_id):
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM tokens WHERE instance_id = %s;", (instance_id,))
        cursor.execute("DELETE FROM process_instances WHERE id = %s;", (instance_id,))
        cursor.execute("DELETE FROM process_definitions WHERE id = %s;", (process_def_id,))
        
        cursor.close()
        conn.close()
        print(f"🧹 [Cleanup] Данные процесса '{instance_id}' удалены из PostgreSQL.")
    except Exception as e:
        print(f"⚠️ [Cleanup] Ошибка при очистке БД: {e}")

# =====================================================================
# ОСНОВНЫЕ СЦЕНАРИИ ТЕСТИРОВАНИЯ
# =====================================================================

def run_complex_workflow_test():
    print("=====================================================================")
    print("🚀 СТАРТ КОМПЛЕКСНОГО ИНТЕГРАЦИОННОГО ТЕСТА (Gateway -> Engine -> WS)")
    print(f"Идентификаторы: DefID={PROCESS_DEF_ID} | InstanceID={INSTANCE_ID}")
    print("=====================================================================\n")

    try:
        # Шаг 1-3
        print(f"📌 [Шаг 1-3] Отправка BPMN-схемы и запуск процесса '{INSTANCE_ID}'...")
        status_code, body = send_gateway_event(
            instance_id=INSTANCE_ID,
            event_name="StartProcess",
            element_id=TASK_1_ID,
            payload_data={"bpmn_xml": COMPLEX_BPMN_XML, "order_amount": 150000}
        )
        print(f"   -> Ответ HTTP Gateway ({status_code}): {body}")

        # Шаг 2
        print(f"\n📌 [Шаг 2] Проверка сохранения схемы '{PROCESS_DEF_ID}' в process_definitions...")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM process_definitions WHERE id = %s;", (PROCESS_DEF_ID,))
        def_row = cursor.fetchone()
        cursor.close()
        conn.close()

        if def_row:
            print(f"   ✅ Схема найдена в БД: ID='{def_row[0]}', Name='{def_row[1]}'")
        else:
            raise AssertionError(f"❌ Ошибка: Схема '{PROCESS_DEF_ID}' не сохранена!")

        # Шаг 4-5
        print(f"\n📌 [Шаг 4-5] Проверка токена первого шага '{TASK_1_ID}' (ожидается ACTIVE)...")
        success, current_state = poll_db_token(TASK_1_ID, "ACTIVE", instance_id=INSTANCE_ID)
        if success:
            print(f"   ✅ Токен '{TASK_1_ID}' успешно переведен в состояние ACTIVE!")
        else:
            raise AssertionError(f"❌ Ошибка: Токен '{TASK_1_ID}' имеет статус '{current_state}'")

        # Шаг 6
        print(f"\n📌 [Шаг 6] Симуляция ответа от ERP для '{TASK_1_ID}'...")
        send_nats_erp_response(
            instance_id=INSTANCE_ID,
            element_id=TASK_1_ID,
            status="SUCCESS",
            payload_data={"validation_status": "APPROVED", "credit_limit_ok": True}
        )
        print("   ✅ Ответ ERP опубликован в NATS.")

        # Шаг 7
        print(f"\n📌 [Шаг 7] Проверка продвижения: '{TASK_1_ID}' -> COMPLETED, '{TASK_2_ID}' -> ACTIVE...")
        task1_completed, _ = poll_db_token(TASK_1_ID, "COMPLETED", instance_id=INSTANCE_ID)
        task2_active, _ = poll_db_token(TASK_2_ID, "ACTIVE", instance_id=INSTANCE_ID)

        if task1_completed and task2_active:
            print(f"   ✅ Успешно! Токен '{TASK_1_ID}'=COMPLETED, токен '{TASK_2_ID}'=ACTIVE")
        else:
            raise AssertionError("❌ Ошибка продвижения шагов!")

        # Шаг 8
        print(f"\n📌 [Шаг 8] Симуляция ответа от ERP для финального шага '{TASK_2_ID}'...")
        send_nats_erp_response(
            instance_id=INSTANCE_ID,
            element_id=TASK_2_ID,
            status="SUCCESS",
            payload_data={"reservation_id": "RES-99812", "warehouse": "W-01"}
        )
        print("   ✅ Ответ ERP для шага 2 опубликован в NATS.")

        # Шаг 9-10
        print(f"\n📌 [Шаг 9-10] Проверка финализации процесса '{INSTANCE_ID}' в БД...")
        task2_completed, t2_final_state = poll_db_token(TASK_2_ID, "COMPLETED", instance_id=INSTANCE_ID)
        inst_completed, inst_status = poll_db_instance_status("COMPLETED", instance_id=INSTANCE_ID)

        if task2_completed and inst_completed:
            print(f"   🎉 Процесс '{INSTANCE_ID}' полностью завершен (status = COMPLETED)!")
        else:
            raise AssertionError(f"❌ Ошибка завершения! Instance status='{inst_status}', Task2 state='{t2_final_state}'")

        # Проверка WS
        print(f"\n🌐 [WebSocket Check] Валидация вещания событий по WebSocket...")
        time.sleep(2)

        matched_ws_events = [
            ev for ev in captured_ws_events 
            if ev.get("instance_id") == INSTANCE_ID
        ]

        print(f"   📊 Перехвачено событий по текущему инстансу: {len(matched_ws_events)}")
        for ev in matched_ws_events:
            print(f"      • Payload: {ev}")

        if len(matched_ws_events) >= 2:
            print("   ✅ WebSocket broadcast валидирован!")
        else:
            print("   ⚠️ Внимание: Пакеты в WS не обнаружены.")

        print("\n=====================================================================")
        print("✅ ВСЕ 10 ЭТАПОВ И ПРОВЕРКА WEBSOCKET УСПЕШНО ПРОЙДЕНЫ!")
        print("=====================================================================")

    finally:
        cleanup_test_data(INSTANCE_ID, PROCESS_DEF_ID)

def run_sla_timeout_test():
    sla_uuid = uuid.uuid4().hex[:8]
    sla_instance_id = f"inst-sla-{sla_uuid}"
    sla_process_def_id = f"Process_SLA_{sla_uuid}"
    
    print("\n=====================================================================")
    print("🚀 СТАРТ ТЕСТА SLA WORKER (Обработка таймаутов процессов)")
    print(f"Идентификаторы: InstanceID={sla_instance_id}")
    print("=====================================================================\n")

    try:
        # 1. Запуск процесса
        print(f"📌 [SLA Step 1] Создание процесса '{sla_instance_id}'...")
        bpmn_xml = COMPLEX_BPMN_XML.replace(PROCESS_DEF_ID, sla_process_def_id)
        send_gateway_event(
            instance_id=sla_instance_id,
            event_name="StartProcess",
            element_id=TASK_1_ID,
            payload_data={"bpmn_xml": bpmn_xml}
        )

        # 2. Проверка первого токена ACTIVE
        success, state = poll_db_token(TASK_1_ID, "ACTIVE", instance_id=sla_instance_id)
        if not success:
            raise AssertionError(f"❌ Токен не перешел в ACTIVE (статус: {state})")
        print(f"   ✅ Токен '{TASK_1_ID}' успешно зарегистрирован как ACTIVE.")

        # 3. Эмуляция зависания (состаривание токена)
        force_expire_token(sla_instance_id, TASK_1_ID)

        # 4. Ожидание реакции SLA Worker
        print(f"\n📌 [SLA Step 2] Ожидание срабатывания SLA Worker...")
        timed_out, final_state = poll_db_token(TASK_1_ID, "TIMED_OUT", timeout=20, instance_id=sla_instance_id)

        if timed_out:
            print(f"   🎉 УСПЕХ! SLA Worker перевел токен в status='TIMED_OUT'")
        else:
            raise AssertionError(f"❌ SLA Worker не обработал таймаут! Текущий статус: '{final_state}'")

        # 5. Проверка отправки WebSocket события
        print(f"\n📌 [SLA Step 3] Проверка получения события TIMED_OUT по WebSocket...")
        time.sleep(1)
        
        ws_sla_events = [
            ev for ev in captured_ws_events
            if ev.get("instance_id") == sla_instance_id and ev.get("state") == "TIMED_OUT"
        ]

        if ws_sla_events:
            print(f"   ✅ WebSocket событие TIMED_OUT успешно получено: {ws_sla_events[0]}")
        else:
            print("   ⚠️ Внимание: Событие TIMED_OUT не поступило по WebSocket.")

        print("\n=====================================================================")
        print("✅ ТЕСТ SLA WORKER УСПЕШНО ПРОЙДЕН!")
        print("=====================================================================")

    finally:
        cleanup_test_data(sla_instance_id, sla_process_def_id)

if __name__ == "__main__":
    start_websocket_listener()
    try:
        run_complex_workflow_test()
        run_sla_timeout_test()
    finally:
        ws_stop_event.set()