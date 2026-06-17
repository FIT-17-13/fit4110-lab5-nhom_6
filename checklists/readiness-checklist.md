# Readiness Checklist - Lab 05

Day la checklist chung cho stack Lab 05 co ca IoT service va Notification service trong cung project.

- [x] **Database ready:** container DB chay on dinh va phan hoi `pg_isready`.
- [x] **AI service ready:** container AI service tra `200` cho `/health` va `POST /predict` hoat dong cho IoT flow.
- [x] **IoT API ready:** container IoT API tra `200` cho `/health` va tao/lay readings thanh cong khi token hop le.
- [x] **Notification API ready:** container Notification API tra `200` cho `/health` va tao/lay notifications thanh cong khi token hop le.
- [x] **Environment variables:** `.env.example` da khai bao day du port, DB, auth token va AI URL cho ca hai service.
- [x] **Network & Ports:** mang `team-internal` hoat dong; `api`, `notification`, `db`, `ai-service` giao tiep noi bo on dinh; ports 8000, 8002, 9000, 5432 duoc map ro rang.
- [x] **Image tags:** image su dung tag `v0.1.0-team-iot` cho IoT/API va AI, `v0.1.0-team-notify` cho Notification.

Ghi chu them:

```
- IoT service dung PostgreSQL va AI mock trong cung stack Compose.
- Notification service cung project, cung DB, co healthcheck rieng va Postman test rieng trong collection chung.
- Newman collection Lab 05 da verify ca IoT va Notification.
```
