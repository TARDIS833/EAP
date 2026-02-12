# excel_Automation Public Patch Repo

이 저장소는 `excel_auto` 프로그램의 공개 패치 파일만 배포합니다.

## 포함 파일
- `manifest.json`
- `configs/excel_auto_mapping.json`
- `configs/custom_rules.json`
- `configs/gift_rules.json`

## 운영 원칙
- 앱 코드는 포함하지 않습니다.
- 규칙(JSON) 변경 시 `patch_version`을 증가시킵니다.
- 앱은 `manifest.json`을 먼저 읽고 해시 검증 후 파일을 적용합니다.

## 사용자 적용 절차(수동)
1. 최신 파일 다운로드
2. 프로그램 폴더의 `configs`에 덮어쓰기
3. 프로그램 재실행
