# PostgreSQL Log Replay Tool

RDS PostgreSQL에서 postgresql.log를 replay하여 쿼리별 성능을 비교하는 도구입니다.

## 기능

- PostgreSQL log 파일 파싱 (단일 라인 및 멀티라인 쿼리 지원)
- 각 쿼리를 대상 RDS PostgreSQL에서 재실행
- 원본 실행 시간과 현재 실행 시간 비교 (밀리초 단위)
- 성능 차이 분석 및 CSV 리포트 생성
- 실행 시간 기준 내림차순 정렬

## 설치

```bash
pip install psycopg2-binary
```

## 설정

1. `pg_config.ini` 파일을 편집하여 PostgreSQL 연결 정보를 입력:

```ini
[postgresql]
host = your-rds-postgresql-endpoint.amazonaws.com
port = 5432
user = postgres
password = your-password
database = your-database-name

[settings]
# 최대 실행할 쿼리 수 (0 = 모든 쿼리)
max_queries = 100
# 쿼리 타임아웃 (초)
query_timeout = 30
# 결과 출력 파일명
output_file = pg_query_performance_report.csv
# 캐시 클리어 여부 (true/false)
clear_cache = false
# 최소 실행 시간 필터 (밀리초)
min_duration_ms = 0
```

## PostgreSQL 로그 설정

PostgreSQL에서 duration 로깅을 활성화해야 합니다:

```sql
-- postgresql.conf 또는 RDS 파라미터 그룹에서 설정
log_min_duration_statement = 0  -- 모든 쿼리 로깅 (밀리초)
log_statement = 'all'           -- 모든 SQL 문 로깅
log_duration = on               -- 실행 시간 로깅
```

## 사용법

```bash
python postgresql_log_replay.py postgresql.log
```

또는 다른 설정 파일 사용:

```bash
python postgresql_log_replay.py postgresql.log -c my_pg_config.ini
```

## 출력

### 콘솔 출력
- 연결 상태 및 진행률
- 실행 결과 요약 통계
- 가장 오래 걸리는 쿼리 TOP 5

### CSV 리포트 (pg_query_performance_report.csv)
- Query_ID: 쿼리 번호
- Line_Number: 로그 파일 라인 번호
- SQL_Query: SQL 쿼리 (200자 제한)
- Original_Duration_MS: 원본 실행 시간 (밀리초)
- Cold_Cache_Time_MS: 첫 번째 실행 시간 (콜드 캐시)
- Avg_Time_MS: 평균 실행 시간 (밀리초)
- Min_Time_MS: 최소 실행 시간 (밀리초)
- Performance_Diff_MS: 성능 차이 (밀리초)
- Performance_Ratio: 성능 비율 (배수)
- Execution_Count: 실행 횟수
- Status: 실행 상태 (성공/실패)

## 지원하는 로그 형식

### 단일 라인 쿼리
```
2024-02-26 10:30:15.123 UTC [12345]: [1-1] user=postgres,db=testdb LOG:  duration: 1234.567 ms  statement: SELECT * FROM users;
```

### 멀티라인 쿼리
```
2024-02-26 10:32:30.789 UTC [12347]: [3-1] user=postgres,db=testdb LOG:  duration: 567.890 ms  statement: SELECT p.id, p.title
2024-02-26 10:32:30.790 UTC [12347]: [3-2] user=postgres,db=testdb LOG:  	FROM posts p
2024-02-26 10:32:30.791 UTC [12347]: [3-3] user=postgres,db=testdb LOG:  	WHERE p.published = true;
```

## 주의사항

1. **읽기 전용 쿼리 권장**: INSERT, UPDATE, DELETE 쿼리는 데이터를 변경할 수 있으므로 주의
2. **테스트 환경 사용**: 프로덕션 환경에서 직접 실행하지 마세요
3. **로그 크기**: 큰 로그 파일은 파싱에 시간이 걸릴 수 있습니다
4. **시스템 쿼리 제외**: pg_stat_, information_schema 등 시스템 쿼리는 자동 제외

## 예제

```bash
# 기본 실행
python postgresql_log_replay.py /path/to/postgresql.log

# 최소 1초 이상 걸린 쿼리만 분석
# pg_config.ini에서 min_duration_ms = 1000으로 설정 후 실행
python postgresql_log_replay.py /path/to/postgresql.log

# 캐시 클리어하며 실행 (정확한 측정)
# pg_config.ini에서 clear_cache = true로 설정 후 실행
python postgresql_log_replay.py /path/to/postgresql.log
```

## 문제 해결

### 연결 실패
- RDS 엔드포인트, 포트, 사용자명, 비밀번호 확인
- 보안 그룹에서 5432 포트 허용 확인
- VPC 및 서브넷 설정 확인

### 로그 파싱 실패
- PostgreSQL 로그 형식 확인
- log_min_duration_statement 설정 확인
- 로그 파일 인코딩 확인 (UTF-8 권장)

### 성능 측정 정확도 향상
- clear_cache = true 설정으로 캐시 효과 제거
- 테스트 환경에서 다른 쿼리 실행 중단
- 여러 번 실행하여 평균값 사용

## PostgreSQL vs MySQL 차이점

- **시간 단위**: PostgreSQL은 밀리초(ms), MySQL은 초(sec)
- **로그 형식**: PostgreSQL은 더 상세한 컨텍스트 정보 포함
- **멀티라인 지원**: PostgreSQL 로그의 복잡한 멀티라인 쿼리 파싱
- **캐시 관리**: PostgreSQL의 shared_buffers와 다른 캐시 메커니즘

## 출력 예

<img width="794" height="364" alt="image" src="https://github.com/user-attachments/assets/a6adc736-8aeb-4ab6-ad90-5cfa21921c00" />
