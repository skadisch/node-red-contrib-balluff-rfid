# Release-Branch Checklist

## Code-Freeze
- [ ] Branch `release/x.y` erstellen (falls noch nicht vorhanden)
- [ ] Bei Minor: Cherry Pick der Commit
- [ ] Release Candidate erstellen
  - [ ] Version anpassen auf `x.y.z-rcn`
  - [ ] Changelog-Kapitel für Version erstellen
  - [ ] Commit Pushen
  - [ ] RC-Release erstellen

## Release
- [ ] Release Preparation Commit erstellen
  - [ ] Version anpassen auf `x.y.z`
  - [ ] Changelog-Kapitel für Version fertigstellen
  - [ ] Commit Pushen
- [ ] Einen neuen Release mit neuem Tag erstellen
  - [ ] Auf dem ***release/x.y*** Branch
  - [ ] ChangeLog in die ReleaseNotes
  - [ ] Package Link hinzufügen
- [ ] Diesen MR mergen (ohne zu Löschen oder zu Squashen)
