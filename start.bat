@echo off
echo Installing dependencies...
python -m pip install -r requirements.txt --quiet

echo.
echo Starting WhatsApp React Automator...
echo Open http://localhost:5000 in your browser
echo Press Ctrl+C to stop
echo.
python app.py
pause
