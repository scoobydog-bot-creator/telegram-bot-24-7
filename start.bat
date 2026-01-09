@echo off
echo Запуск бота для балансировки складов...
echo.

REM Проверка наличия Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Ошибка: Python не найден!
    pause
    exit /b 1
)

REM Проверка установленных зависимостей
echo Проверяю зависимости...
python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo Ошибка: Библиотеки не установлены!
    echo Сначала запустите install.bat
    pause
    exit /b 1
)

REM Проверка наличия файла бота
if not exist "warehouse_bot.py" (
    echo Ошибка: Файл warehouse_bot.py не найден!
    pause
    exit /b 1
)

echo Запускаю бота...
echo ========================================
echo БОТ ДЛЯ БАЛАНСИРОВКИ СКЛАДОВ
echo ========================================
echo.
python warehouse_bot.py

if errorlevel 1 (
    echo.
    echo Бот завершил работу с ошибкой.
    pause
    exit /b 1
)

echo.
pause