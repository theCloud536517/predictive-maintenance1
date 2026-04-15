#include <Predictive_maintenance3_inferencing.h>
#include <SPI.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_ADXL345_U.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>

// --- CẤU HÌNH THINGSBOARD ---
#define TB_SERVER       "thingsboard.cloud"
#define TB_TOKEN        "Dfl4ac6IaTNLZvz24qoe"
#define TB_PORT         1883

// --- CẤU HÌNH PHẦN CỨNG ---
#define ADXL345_CS    10
#define ADXL345_MOSI  11
#define ADXL345_SCK   12
#define ADXL345_MISO  13
#define ONE_WIRE_BUS  4
#define RELAY_PIN     5

#define LED_GREEN_PIN 42
#define LED_RED_PIN   2
#define BUZZER_PIN    1

// --- THAM SỐ LOGIC CẢI THIỆN ---
// Ngưỡng confidence tối thiểu để AI xác nhận lỗi (tránh noise)
#define AI_CONFIDENCE_THRESHOLD   0.70f   // 70%
// Ngưỡng nhiệt độ với hysteresis để tránh dao động
#define TEMP_OVER_THRESH          60.0f
#define TEMP_CLEAR_THRESH         57.0f   // Phải giảm xuống 57°C mới hết cảnh báo
// Ngắt khẩn cấp sau N lần lỗi liên tiếp (giảm từ 20 → 15 để nhạy hơn)
#define CONSECUTIVE_TRIP_COUNT    15

// --- MẠNG VÀ ĐÁM MÂY ---
char ssid[] = "Hongha";
char pass[] = "0936387676";

WiFiClient espClient;
PubSubClient client(espClient);

Adafruit_ADXL345_Unified accel = Adafruit_ADXL345_Unified(
    ADXL345_SCK, ADXL345_MISO, ADXL345_MOSI, ADXL345_CS, 12345);
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);

// --- BỘ ĐỆM ---
#define BUFFER_SIZE EI_CLASSIFIER_DSP_INPUT_FRAME_SIZE
float ring_buffer[BUFFER_SIZE];
float features[BUFFER_SIZE];
volatile int  ring_index   = 0;
volatile bool buffer_ready = false;

// --- BIẾN TRẠNG THÁI ---
volatile float  current_temp           = 0.0;
volatile bool   relay_state            = true;
volatile bool   safety_trip            = false;
volatile int    consecutive_bad        = 0;   
volatile int    good_streak            = 0;   
volatile int    bad_count_1d           = 0;
volatile bool   overheat_flag          = false; 

volatile float  unbalance_prob_global  = 0.0;
String          current_ai_state       = "Khoi tao";
unsigned long   last_1d_reset          = 0;

// SMA (Simple Moving Average) 5 mẫu để lọc nhiễu AI
#define SMA_WINDOW 5
float sma_bad_scores[SMA_WINDOW] = {0};
int   sma_index = 0;

// --- SEMAPHORE ---
SemaphoreHandle_t mqttSemaphore;

TaskHandle_t SamplingTaskHandle;
TaskHandle_t TempTaskHandle;
TaskHandle_t MQTTTaskHandle;
TaskHandle_t AlarmTaskHandle;

// -------------------------------------------------------
// HELPER: Tính trung bình trượt (SMA) điểm lỗi
// -------------------------------------------------------
float updateSMA(float new_score) {
    sma_bad_scores[sma_index] = new_score;
    sma_index = (sma_index + 1) % SMA_WINDOW;
    float sum = 0;
    for (int i = 0; i < SMA_WINDOW; i++) sum += sma_bad_scores[i];
    return sum / SMA_WINDOW;
}

// -------------------------------------------------------
// CALLBACK: Nhận lệnh RPC từ ThingsBoard
// -------------------------------------------------------
void callback(char* topic, byte* payload, unsigned int length) {
    StaticJsonDocument<200> doc;
    deserializeJson(doc, payload, length);
    String methodName = doc["method"].as<String>();

    if (methodName == "setRelayStatus") {
        relay_state = doc["params"];
        if (relay_state) {
            safety_trip       = false;
            consecutive_bad   = 0;
            good_streak       = 0;
        }
        digitalWrite(RELAY_PIN, relay_state ? HIGH : LOW);

        // Phản hồi RPC
        String reqId    = String(topic).substring(26);
        String outTopic = "v1/devices/me/rpc/response/" + reqId;
        client.publish(outTopic.c_str(), relay_state ? "true" : "false");
        xSemaphoreGive(mqttSemaphore); // Gửi telemetry ngay để cập nhật giao diện
    }
    else if (methodName == "getRelayStatus") {
        String reqId    = String(topic).substring(26);
        String outTopic = "v1/devices/me/rpc/response/" + reqId;
        client.publish(outTopic.c_str(), relay_state ? "true" : "false");
    }
}

// -------------------------------------------------------
// TÁC VỤ ALARM (Core 1, Priority 3)
// -------------------------------------------------------
void TaskAlarm(void *pvParameters) {
    bool blink_state = false;
    for (;;) {
        if (safety_trip) {
            digitalWrite(LED_GREEN_PIN, LOW);
            digitalWrite(LED_RED_PIN,   HIGH);
            digitalWrite(BUZZER_PIN,    HIGH);
            vTaskDelay(pdMS_TO_TICKS(100));
        }
        else if (consecutive_bad > 0) {
            int blink_ms = (consecutive_bad > 10) ? 150 : 300;
            digitalWrite(LED_GREEN_PIN, LOW);
            blink_state = !blink_state;
            digitalWrite(LED_RED_PIN,   blink_state ? HIGH : LOW);
            digitalWrite(BUZZER_PIN,    blink_state ? HIGH : LOW);
            vTaskDelay(pdMS_TO_TICKS(blink_ms));
        }
        else {
            digitalWrite(LED_GREEN_PIN, HIGH);
            digitalWrite(LED_RED_PIN,   LOW);
            digitalWrite(BUZZER_PIN,    LOW);
            vTaskDelay(pdMS_TO_TICKS(100));
        }
    }
}

// -------------------------------------------------------
// TÁC VỤ LẤY MẪU 200Hz (Core 0, Priority 3)
// -------------------------------------------------------
void TaskSampling(void *pvParameters) {
    TickType_t xLastWakeTime = xTaskGetTickCount();
    const TickType_t xFrequency = pdMS_TO_TICKS(5);
    for (;;) {
        vTaskDelayUntil(&xLastWakeTime, xFrequency);
        sensors_event_t event;
        accel.getEvent(&event);
        ring_buffer[ring_index]     = event.acceleration.x;
        ring_buffer[ring_index + 1] = event.acceleration.y;
        ring_buffer[ring_index + 2] = event.acceleration.z;
        ring_index += 3;
        if (ring_index >= BUFFER_SIZE) {
            ring_index   = 0;
            buffer_ready = true;
        }
    }
}

// -------------------------------------------------------
// TÁC VỤ NHIỆT ĐỘ (Core 0, Priority 1)
// -------------------------------------------------------
void TaskTemperature(void *pvParameters) {
    sensors.begin();
    sensors.setWaitForConversion(false);
    for (;;) {
        sensors.requestTemperatures();
        vTaskDelay(pdMS_TO_TICKS(750));
        float t = sensors.getTempCByIndex(0);
        if (t != 85.0f && t != 127.0f && t != -127.0f && t > -40.0f) {
            current_temp = t;
        }
        vTaskDelay(pdMS_TO_TICKS(1250));
    }
}

// -------------------------------------------------------
// TÁC VỤ MQTT - GỬI GÓI TIN TỐI GIẢN (Core 0, Priority 2)
// -------------------------------------------------------
void TaskMQTT(void *pvParameters) {
    client.setServer(TB_SERVER, TB_PORT);
    client.setCallback(callback);
    client.setKeepAlive(60);         
    client.setSocketTimeout(10);
    client.setBufferSize(300); // Tối ưu bộ đệm cho gói tin vừa phải

    unsigned long last_reconnect = 0;

    for (;;) {
        if (!client.connected()) {
            unsigned long now = millis();
            if (now - last_reconnect > 3000) {
                last_reconnect = now;
                Serial.print("MQTT reconnecting...");
                if (client.connect("ESP32S3_PM_v2", TB_TOKEN, NULL)) {
                    Serial.println(" OK");
                    client.subscribe("v1/devices/me/rpc/request/+");
                } else {
                    Serial.print(" FAIL rc=");
                    Serial.println(client.state());
                }
            }
            vTaskDelay(pdMS_TO_TICKS(100));
            continue;
        }

        client.loop();

        // Gửi dữ liệu khi có tín hiệu (Semaphore) từ Core 1
        if (xSemaphoreTake(mqttSemaphore, pdMS_TO_TICKS(1500)) == pdTRUE
            || true) {

            if (!client.connected()) {
                vTaskDelay(pdMS_TO_TICKS(5));
                continue;
            }

            StaticJsonDocument<256> jsonDoc; // JSON gọn nhẹ
            jsonDoc["temperature"]    = (float)((int)(current_temp * 10)) / 10.0;
            jsonDoc["ai_state"]       = current_ai_state;
            jsonDoc["bad_count"]      = bad_count_1d;
            jsonDoc["relay_state"]    = relay_state;
            jsonDoc["unbalance_prob"] = (float)((int)(unbalance_prob_global * 10)) / 10.0;

            // Xử lý cảnh báo chữ
            if (safety_trip) {
                jsonDoc["status_msg"] = "DA NGAT DONG CO";
            } else if (consecutive_bad >= 8) {
                jsonDoc["status_msg"] = "NGUY HIEM: " + String(consecutive_bad) + "/15";
            } else if (consecutive_bad > 0) {
                jsonDoc["status_msg"] = "CANH BAO LOI";
            } else {
                jsonDoc["status_msg"] = "HE THONG ON DINH";
            }

            char buffer[256];
            serializeJson(jsonDoc, buffer);

            bool ok = client.publish("v1/devices/me/telemetry", buffer, false);
            if (!ok) {
                xSemaphoreGive(mqttSemaphore); // Thử lại nếu rớt gói tin
            }
        }

        vTaskDelay(pdMS_TO_TICKS(5)); 
    }
}

// -------------------------------------------------------
// SETUP
// -------------------------------------------------------
void setup() {
    Serial.begin(115200);

    pinMode(RELAY_PIN,     OUTPUT); digitalWrite(RELAY_PIN,     HIGH);
    pinMode(LED_GREEN_PIN, OUTPUT); digitalWrite(LED_GREEN_PIN, HIGH);
    pinMode(LED_RED_PIN,   OUTPUT); digitalWrite(LED_RED_PIN,   LOW);
    pinMode(BUZZER_PIN,    OUTPUT); digitalWrite(BUZZER_PIN,    LOW);

    if (!accel.begin()) {
        Serial.println("LOI ADXL345!");
        while (1) {
            digitalWrite(LED_RED_PIN, HIGH);
            digitalWrite(BUZZER_PIN,  HIGH); delay(200);
            digitalWrite(LED_RED_PIN, LOW);
            digitalWrite(BUZZER_PIN,  LOW);  delay(200);
        }
    }
    accel.setRange(ADXL345_RANGE_16_G);
    accel.setDataRate(ADXL345_DATARATE_3200_HZ);

    mqttSemaphore = xSemaphoreCreateBinary();
    xSemaphoreGive(mqttSemaphore); 

    WiFi.begin(ssid, pass);
    Serial.print("Connecting WiFi");
    while (WiFi.status() != WL_CONNECTED) {
        delay(500); Serial.print(".");
    }
    Serial.println("\nWiFi OK. Starting RTOS.");

    xTaskCreatePinnedToCore(TaskSampling,   "Samp",  4096, NULL, 3, &SamplingTaskHandle, 0);
    xTaskCreatePinnedToCore(TaskTemperature,"Temp",  2048, NULL, 1, &TempTaskHandle,     0);
    xTaskCreatePinnedToCore(TaskMQTT,       "MQTT",  8192, NULL, 2, &MQTTTaskHandle,     0);
    xTaskCreatePinnedToCore(TaskAlarm,      "Alarm", 2048, NULL, 3, &AlarmTaskHandle,    1);
}

// -------------------------------------------------------
// CORE 1 loop(): AI + Logic bảo vệ 
// -------------------------------------------------------
void loop() {
    if (!buffer_ready) {
        vTaskDelay(pdMS_TO_TICKS(10));
        return;
    }

    int snap_idx = ring_index;
    for (int i = 0; i < BUFFER_SIZE; i += 3) {
        int src       = (snap_idx + i) % BUFFER_SIZE;
        features[i]   = ring_buffer[src];
        features[i+1] = ring_buffer[src + 1];
        features[i+2] = ring_buffer[src + 2];
    }
    buffer_ready = false;

    signal_t signal;
    if (numpy::signal_from_buffer(features, BUFFER_SIZE, &signal) != 0) return;
    ei_impulse_result_t result = {0};
    if (run_classifier(&signal, &result, false) != EI_IMPULSE_OK) return;

    float max_value = 0.0;
    int   max_idx   = -1;
    float unbalance_prob = 0.0;

    for (uint16_t i = 0; i < EI_CLASSIFIER_LABEL_COUNT; i++) {
        String lbl = String(result.classification[i].label);
        float  val = result.classification[i].value;

        if (lbl.equalsIgnoreCase("unbalance")) unbalance_prob = val * 100.0f;
        if (val > max_value) { max_value = val; max_idx = i; }
    }

    if (max_idx >= 0 && max_value >= AI_CONFIDENCE_THRESHOLD) {
        current_ai_state = String(result.classification[max_idx].label);
    } else {
        current_ai_state = "uncertain"; 
    }
    unbalance_prob_global = unbalance_prob;

    if (!overheat_flag && current_temp > TEMP_OVER_THRESH) {
        overheat_flag = true;   
    } else if (overheat_flag && current_temp < TEMP_CLEAR_THRESH) {
        overheat_flag = false;  
    }

    bool is_bad_vibration = (current_ai_state.equalsIgnoreCase("unbalance") ||
                             current_ai_state.equalsIgnoreCase("obstacle"))
                            && (max_value >= AI_CONFIDENCE_THRESHOLD);

    float bad_score = 0.0f;
    if (is_bad_vibration) bad_score += max_value;          
    if (overheat_flag)    bad_score += 0.5f;               

    float sma_score = updateSMA(bad_score);
    bool  is_bad    = (sma_score > 0.35f) && relay_state;  

    unsigned long now = millis();
    if (now - last_1d_reset > 86400000UL) {
        bad_count_1d  = 0;
        last_1d_reset = now;
    }

    if (is_bad) {
        consecutive_bad++;
        bad_count_1d++;
        good_streak = 0;    
        xSemaphoreGive(mqttSemaphore); 
    } else {
        good_streak++;
        if (good_streak >= 5 && consecutive_bad > 0) {
            consecutive_bad--;
            good_streak = 0;
        }
    }

    if (consecutive_bad >= CONSECUTIVE_TRIP_COUNT && relay_state) {
        relay_state     = false;
        safety_trip     = true;
        consecutive_bad = 0;
        digitalWrite(RELAY_PIN, LOW);
        Serial.println("!!! EMERGENCY TRIP !!!");
        xSemaphoreGive(mqttSemaphore); 
    }

    static unsigned long last_log = 0;
    if (now - last_log > 500) {
        last_log = now;
        Serial.printf("[AI] %s (%.0f%%) | Temp: %.1f°C | Bad: %d/%d | SMA: %.2f\n",
            current_ai_state.c_str(), max_value * 100,
            current_temp, consecutive_bad, CONSECUTIVE_TRIP_COUNT, sma_score);
    }
}