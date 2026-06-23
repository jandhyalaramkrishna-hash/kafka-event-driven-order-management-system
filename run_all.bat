@echo off
echo =================================================================
echo   STARTING OMS EVENT-DRIVEN SERVICES ENGINE & WEBSITE
echo =================================================================

:: 1. Launch FastAPI Server in a separate window (Runs from root folder)
start "FastAPI Host Server" powershell -NoExit -Command "python -m uvicorn app:app --reload"

:: 🛠️ INCREASED PAUSE: Wait 5 seconds to guarantee the server is live before opening the browser
echo Waiting 5 seconds for the FastAPI host server to initialize...
timeout /t 5 /nobreak >nul

:: 2. Launch Microservice Components from the 'services' subfolder
start "1. Order Service" powershell -NoExit -Command "python services\order_service.py"
start "2. Kitchen Engine & Live Inventory" powershell -NoExit -Command "python services\kitchen_service.py"
start "3. Delivery Service" powershell -NoExit -Command "python services\delivery_service.py"
start "4. Dead Letter Queue" powershell -NoExit -Command "python services\dlq_service.py"
start "5. Duplicate Monitor" powershell -NoExit -Command "python services\duplicate_service.py"

:: 3. Automatically open the trigger link in the default browser
echo Launching default web browser to dispatch order events...
start http://127.0.0.1:8000/orders

echo All components successfully initialized!
pause