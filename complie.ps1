$ClientExe = "..\scidb-client.exe"
$BuildDir = ".\build\"
$DistDir = ".\dist\"
$EggInfoDir = ".\oxdc_scidb.egg-info\"
$SpecFile = ".\client.spec"

python setup.py sdist bdist_wheel
twine upload dist/*
Remove-Item -Recurse -Force -Path $BuildDir, $DistDir, $EggInfoDir
pyinstaller --onefile ".\client.py"

if (Test-Path $ClientExe) {
  Remove-Item $ClientExe
}
Move-Item -Force -Path ".\dist\client.exe" -Destination $ClientExe
Remove-Item -Recurse -Force -Path ".\__pycache__", $BuildDir, $DistDir, $SpecFile
