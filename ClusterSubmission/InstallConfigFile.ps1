param($installPath, $toolsPath, $package, $project)

foreach ($targetsFile in Get-ChildItem (Join-Path $installPath "build") -Filter "*.targets") {
  (Get-Content $targetsFile.FullName) | Foreach-Object { $_ -replace 'SUBMISSION_PACKAGE_DIRECTORY', $installPath } | Set-Content $targetsFile.FullName
}