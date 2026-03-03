#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL Log Replay Tool
RDS PostgreSQL에서 postgresql.log를 replay하여 성능을 비교하는 도구
"""

import re
import time
import csv
import argparse
import configparser
import psycopg2
from psycopg2 import Error, sql
from datetime import datetime
import sys
import os
import logging

class PostgreSQLLogReplay:
    def __init__(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.connection = None
        self.queries = []
        
    def connect_to_postgresql(self):
        """PostgreSQL 데이터베이스에 연결"""
        try:
            self.connection = psycopg2.connect(
                host=self.config['postgresql']['host'],
                port=int(self.config['postgresql']['port']),
                user=self.config['postgresql']['user'],
                password=self.config['postgresql']['password'],
                database=self.config['postgresql']['database'],
                connect_timeout=10
            )
            
            self.connection.autocommit = True
            print(f"PostgreSQL 연결 성공: {self.config['postgresql']['host']}")
            return True
                
        except Error as e:
            print(f"PostgreSQL 연결 실패: {e}")
            return False
    
    def parse_postgresql_log(self, log_file):
        """PostgreSQL log 파일을 파싱하여 쿼리 추출"""
        print(f"PostgreSQL log 파싱 중: {log_file}")
        
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        
        # 실제 PostgreSQL 로그 형식에 맞는 패턴
        # 2026-03-03 03:19:13 UTC:172.31.90.5(38902):postgres@postgres:[29979]:LOG:  duration: 1334.640 ms  statement: select count(*) from t2;
        log_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC:.*?LOG:\s+duration:\s+([\d.]+)\s+ms\s+statement:\s*(.*)'
        
        min_duration = float(self.config.get('settings', 'min_duration_ms', fallback=0))
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            
            print(f"DEBUG: 라인 {line_num} 처리 중: {line[:100]}...")  # 디버그 출력
            
            # 로그 라인 매칭
            match = re.search(log_pattern, line)
            if match:
                timestamp = match.group(1)
                duration_ms = float(match.group(2))
                statement = match.group(3).strip()
                
                print(f"DEBUG: 매칭 성공 - duration: {duration_ms}, statement: {statement[:50]}...")
                
                # 최소 duration 필터
                if duration_ms < min_duration:
                    print(f"DEBUG: duration 필터로 제외됨 ({duration_ms} < {min_duration})")
                    continue
                
                # 세미콜론 제거
                if statement.endswith(';'):
                    statement = statement[:-1]
                
                # SQL 쿼리 타입 확인
                sql_upper = statement.upper().strip()
                print(f"DEBUG: SQL 타입 확인 - {sql_upper[:30]}...")
                
                if any(sql_upper.startswith(cmd) for cmd in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH']):
                    print(f"DEBUG: SQL 타입 매칭됨")
                    # 시스템 쿼리 제외 (더 정확한 필터링 - 문장 시작 부분만 확인)
                    system_keywords = [
                        'PG_STAT_', 'PG_CLASS', 'INFORMATION_SCHEMA', 'PG_CATALOG'
                    ]
                    
                    # 시스템 명령어는 문장 시작 부분만 확인
                    system_commands = ['SHOW ', 'SET ', 'BEGIN', 'COMMIT', 'ROLLBACK', 'ANALYZE', 'VACUUM']
                    
                    is_system_query = False
                    
                    # 시스템 키워드 확인 (어디든 포함되면 제외)
                    for keyword in system_keywords:
                        if keyword in sql_upper:
                            is_system_query = True
                            print(f"DEBUG: 시스템 쿼리 키워드 발견: {keyword}")
                            break
                    
                    # 시스템 명령어 확인 (문장 시작 부분만)
                    if not is_system_query:
                        for command in system_commands:
                            if sql_upper.startswith(command):
                                is_system_query = True
                                print(f"DEBUG: 시스템 명령어 발견: {command}")
                                break
                    
                    if not is_system_query:
                        self.queries.append({
                            'sql': statement,
                            'original_duration_ms': duration_ms,
                            'line_number': line_num,
                            'timestamp': timestamp
                        })
                        print(f"DEBUG: 파싱된 쿼리 - 라인 {line_num}: {statement[:50]}...")  # 디버그 출력
                    else:
                        print(f"DEBUG: 시스템 쿼리로 제외됨")
                else:
                    print(f"DEBUG: SQL 타입이 매칭되지 않음")
            else:
                print(f"DEBUG: 정규식 매칭 실패")
        
        print(f"총 {len(self.queries)}개의 쿼리를 파싱했습니다.")
        return len(self.queries)
    
    def execute_query_with_timing(self, sql, clear_cache=False):
        """쿼리를 실행하고 실행 시간을 측정"""
        if not self.connection or self.connection.closed:
            if not self.connect_to_postgresql():
                return None, "연결 실패"
        
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            # 캐시 클리어 옵션 (주의: 프로덕션에서 사용 금지)
            if clear_cache:
                try:
                    # PostgreSQL 캐시 클리어
                    cursor.execute("DISCARD ALL")
                    # 시스템 캐시는 권한이 필요하므로 시도만 함
                    try:
                        cursor.execute("SELECT pg_prewarm_reset()")
                    except:
                        pass
                except Error:
                    pass  # 권한이 없으면 무시
            
            # 쿼리 실행 시간 측정
            start_time = time.time()
            cursor.execute(sql)
            
            # SELECT 쿼리인 경우 결과를 가져와야 실제 실행 완료
            if sql.upper().strip().startswith(('SELECT', 'WITH')):
                results = cursor.fetchall()
                row_count = len(results)
            else:
                row_count = cursor.rowcount
                
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000  # 밀리초로 변환
            
            return execution_time_ms, None
            
        except Error as e:
            error_msg = str(e)
            if "timeout" in error_msg.lower() or "canceling statement" in error_msg.lower():
                return None, "타임아웃"
            return None, f"실행 오류: {error_msg}"
        except Exception as e:
            return None, f"예상치 못한 오류: {str(e)}"
        finally:
            if cursor:
                cursor.close()
    
    def replay_queries(self):
        """쿼리들을 replay하고 성능 비교"""
        if not self.queries:
            print("파싱된 쿼리가 없습니다.")
            return []
        
        max_queries = int(self.config.get('settings', 'max_queries', fallback=0))
        clear_cache = self.config.getboolean('settings', 'clear_cache', fallback=False)
        
        if max_queries > 0:
            queries_to_process = self.queries[:max_queries]
        else:
            queries_to_process = self.queries
        
        print(f"{len(queries_to_process)}개의 쿼리를 실행합니다...")
        if clear_cache:
            print("주의: 캐시 클리어 옵션이 활성화되어 있습니다. (성능에 영향)")
        
        results = []
        
        for i, query_info in enumerate(queries_to_process, 1):
            print(f"진행률: {i}/{len(queries_to_process)} ({i/len(queries_to_process)*100:.1f}%)")
            
            sql = query_info['sql']
            original_duration_ms = query_info['original_duration_ms']
            
            # 여러 번 실행하여 평균 계산 (더 정확한 측정)
            execution_times = []
            errors = []
            
            # 3번 실행하여 평균 계산
            for attempt in range(3):
                current_time_ms, error = self.execute_query_with_timing(sql, clear_cache and attempt == 0)
                
                if error:
                    errors.append(error)
                else:
                    execution_times.append(current_time_ms)
                
                # 캐시 효과를 보기 위해 잠시 대기
                if attempt < 2:
                    time.sleep(0.1)
            
            # 결과 계산
            if execution_times:
                # 첫 번째 실행 (콜드 캐시)
                cold_time_ms = execution_times[0] if len(execution_times) > 0 else None
                # 평균 실행 시간 (웜 캐시)
                avg_time_ms = sum(execution_times) / len(execution_times)
                # 최소 실행 시간 (최적 상태)
                min_time_ms = min(execution_times)
                
                current_time_ms = avg_time_ms
                error = None
            else:
                cold_time_ms = None
                avg_time_ms = None
                min_time_ms = None
                current_time_ms = -1
                error = errors[0] if errors else "알 수 없는 오류"
            
            # 성능 차이 계산
            if current_time_ms is not None and current_time_ms > 0:
                performance_diff_ms = current_time_ms - original_duration_ms
                performance_ratio = current_time_ms / original_duration_ms if original_duration_ms > 0 else float('inf')
            else:
                performance_diff_ms = None
                performance_ratio = None
            
            result = {
                'query_id': i,
                'sql': sql[:200] + '...' if len(sql) > 200 else sql,
                'full_sql': sql,
                'original_duration_ms': original_duration_ms,
                'cold_time_ms': cold_time_ms,
                'avg_time_ms': avg_time_ms,
                'min_time_ms': min_time_ms,
                'current_time_ms': current_time_ms if current_time_ms is not None else -1,
                'performance_diff_ms': performance_diff_ms,
                'performance_ratio': performance_ratio,
                'line_number': query_info['line_number'],
                'timestamp': query_info.get('timestamp', 'N/A'),
                'error': error,
                'execution_count': len(execution_times)
            }
            
            results.append(result)
        
        return results
    
    def generate_report(self, results, output_file):
        """결과를 CSV 파일로 출력"""
        if not results:
            print("생성할 결과가 없습니다.")
            return
        
        # 현재 실행 시간 기준으로 내림차순 정렬 (오래 걸리는 순)
        sorted_results = sorted(results, 
                              key=lambda x: x['current_time_ms'] if x['current_time_ms'] > 0 else 0, 
                              reverse=True)
        
        # CSV 파일 생성
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'Query_ID', 'Line_Number', 'Timestamp', 'SQL_Query', 'Original_Duration_MS', 'Cold_Cache_Time_MS',
                'Avg_Time_MS', 'Min_Time_MS', 'Performance_Diff_MS', 'Performance_Ratio',
                'Execution_Count', 'Status'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in sorted_results:
                status = "성공" if result['error'] is None else result['error']
                
                writer.writerow({
                    'Query_ID': result['query_id'],
                    'Line_Number': result['line_number'],
                    'Timestamp': result.get('timestamp', 'N/A'),
                    'SQL_Query': result['sql'],
                    'Original_Duration_MS': f"{result['original_duration_ms']:.3f}",
                    'Cold_Cache_Time_MS': f"{result['cold_time_ms']:.3f}" if result['cold_time_ms'] is not None else "N/A",
                    'Avg_Time_MS': f"{result['avg_time_ms']:.3f}" if result['avg_time_ms'] is not None else "실패",
                    'Min_Time_MS': f"{result['min_time_ms']:.3f}" if result['min_time_ms'] is not None else "N/A",
                    'Performance_Diff_MS': f"{result['performance_diff_ms']:+.3f}" if result['performance_diff_ms'] is not None else "N/A",
                    'Performance_Ratio': f"{result['performance_ratio']:.2f}x" if result['performance_ratio'] is not None else "N/A",
                    'Execution_Count': result['execution_count'],
                    'Status': status
                })
        
        print(f"\n결과가 {output_file}에 저장되었습니다.")
        
        # 요약 통계 출력
        self.print_summary(sorted_results)
    
    def print_summary(self, results):
        """실행 결과 요약 출력"""
        total_queries = len(results)
        successful_queries = len([r for r in results if r['error'] is None])
        failed_queries = total_queries - successful_queries
        
        if successful_queries > 0:
            successful_results = [r for r in results if r['error'] is None and r['current_time_ms'] > 0]
            
            avg_original = sum(r['original_duration_ms'] for r in successful_results) / len(successful_results)
            avg_current = sum(r['current_time_ms'] for r in successful_results) / len(successful_results)
            
            faster_queries = len([r for r in successful_results if r['current_time_ms'] < r['original_duration_ms']])
            slower_queries = len([r for r in successful_results if r['current_time_ms'] > r['original_duration_ms']])
        
        print("\n" + "="*60)
        print("실행 결과 요약")
        print("="*60)
        print(f"총 쿼리 수: {total_queries}")
        print(f"성공한 쿼리: {successful_queries}")
        print(f"실패한 쿼리: {failed_queries}")
        
        if successful_queries > 0:
            print(f"\n평균 실행 시간:")
            print(f"  원본: {avg_original:.3f}ms")
            print(f"  현재: {avg_current:.3f}ms")
            print(f"  차이: {avg_current - avg_original:+.3f}ms")
            
            print(f"\n성능 비교:")
            print(f"  더 빨라진 쿼리: {faster_queries}개")
            print(f"  더 느려진 쿼리: {slower_queries}개")
            
            # 상위 5개 느린 쿼리 표시
            print(f"\n가장 오래 걸리는 쿼리 TOP 5:")
            for i, result in enumerate(results[:5], 1):
                if result['current_time_ms'] > 0:
                    print(f"  {i}. {result['current_time_ms']:.3f}ms - {result['sql'][:80]}...")
    
    def close_connection(self):
        """데이터베이스 연결 종료"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            print("PostgreSQL 연결이 종료되었습니다.")

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Log Replay Tool')
    parser.add_argument('postgresql_log', help='PostgreSQL log 파일 경로')
    parser.add_argument('-c', '--config', default='pg_config.ini', help='설정 파일 경로 (기본값: pg_config.ini)')
    
    args = parser.parse_args()
    
    # 설정 파일 확인
    if not os.path.exists(args.config):
        print(f"설정 파일을 찾을 수 없습니다: {args.config}")
        print("pg_config.ini 파일을 생성하고 PostgreSQL 연결 정보를 입력해주세요.")
        return 1
    
    # PostgreSQL log 파일 확인
    if not os.path.exists(args.postgresql_log):
        print(f"PostgreSQL log 파일을 찾을 수 없습니다: {args.postgresql_log}")
        return 1
    
    # Replay 도구 초기화
    replay_tool = PostgreSQLLogReplay(args.config)
    
    try:
        # PostgreSQL 연결
        if not replay_tool.connect_to_postgresql():
            return 1
        
        # PostgreSQL log 파싱
        query_count = replay_tool.parse_postgresql_log(args.postgresql_log)
        if query_count == 0:
            print("파싱할 수 있는 쿼리가 없습니다.")
            return 1
        
        # 쿼리 실행 및 성능 측정
        results = replay_tool.replay_queries()
        
        # 결과 리포트 생성
        output_file = replay_tool.config.get('settings', 'output_file', fallback='pg_query_performance_report.csv')
        replay_tool.generate_report(results, output_file)
        
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
        return 1
    except Exception as e:
        print(f"예상치 못한 오류가 발생했습니다: {e}")
        return 1
    finally:
        replay_tool.close_connection()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())