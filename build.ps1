. "$PSScriptRoot\build-system\scripts\getReleaseNotes.ps1"
. "$PSScriptRoot\build-system\scripts\bumpVersion.ps1"

######################################################################
# Step 1: Grab release notes and update solution metadata
######################################################################
$releaseNotes = Get-ReleaseNotes -MarkdownFile (Join-Path -Path $PSScriptRoot -ChildPath "RELEASE_NOTES.md")

# inject release notes into Directory.Generated.props
UpdateVersionAndReleaseNotes -ReleaseNotesResult $releaseNotes -XmlFilePath (Join-Path -Path $PSScriptRoot -ChildPath "src\Directory.Generated.props") 

Write-Output "Added release notes $releaseNotes"