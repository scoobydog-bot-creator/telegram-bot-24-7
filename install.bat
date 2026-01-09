@echo off
echo Установка зависимостей для бота балансировки складов...
echo.

REM Проверка наличия Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Ошибка: Python не найден!
    echo Установите Python 3.8 или выше с сайта python.org
    pause
    exit /b 1
)

REM Проверка наличия pip
pip --version >nul 2>&1
if errorlevel 1 (
    echo Ошибка: pip не найден!
    echo Установите pip для вашей версии Python
    pause
    exit /b 1
)

REM Установка зависимостей
echo Устанавливаю зависимости из requirements.txt...
pip install --upgrade pip
pip install -r requirements.txt

if errorlevel 1 (
    echo Ошибка при установке зависимостей!
    pause
    exit /b 1
)

echo.
echo Установка завершена успешно!
echo.
echo Для запуска бота выполните:
echo 1. Замените токен в файле warehouse_bot.py
echo 2. Запустите start.bat
echo.
pause