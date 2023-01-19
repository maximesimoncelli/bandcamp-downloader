Add-Type -AssemblyName System.IO.Compression.FileSystem

$env = @{}
Get-Content .env | Select-String '^[^#]' | ForEach-Object {
    $kv = $_.Line -split '='
    $env[$kv[0].Trim()] = $kv[1].Trim()
}
$path = $env.EXTRACTION_PATH

$total = (Get-ChildItem -Path $path -Filter *.zip -Recurse).Count

$errors = @()
$i = 0
Get-ChildItem -Path $path -Filter *.zip -Recurse | ForEach-Object {
    $zip = $_.FullName
    $folderName = ($_.Name -split ' - ')
    $subFolderName = ($folderName[1] -split '.zip')[0]
    $destination = Join-Path $path (Join-Path $folderName[0] $subFolderName)
    try {
        [System.IO.Compression.ZipFile]::ExtractToDirectory($zip, $destination)    
        $i += 1
        Write-Progress -Activity "Unzipping files" -Status "Processing $zip" -PercentComplete ($i / $total * 100)
        Remove-Item $zip
    } catch {
        $errors += $zip
    }
}

Write-Host "Files in error: " -ForegroundColor DarkRed
$errors | ForEach-Object {
    Write-Host $_ -BackgroundColor DarkRed
}