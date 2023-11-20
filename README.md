# yes24_chunjae
## 파이프라인 개발

* main.py
* setting.py : DB setting과 관련된 정보. 보안상 문제로 삭제
* db/ 

 
	 ├ __init__.py				

 	├ connector.py				

	 ├ queries_ddb.py				

 	├ queries_rdb.py				

* pipeline/ 

	 ├ __init__.py				

	 ├ controller.py				

	 ├ extract.py				

	 ├ load.py				

	 ├ transform.py				
				
## 📌 프로젝트 목적
* 온라인 채널 yes24(mongodb)에서 판매된 천재도서 데이터를 postgredb에 넣고자 함

## 📌 프로젝트 개요
* 'yes24' 데이터베이스에서 'math_book' 컬렉션의 "영역" field 값이 "연산"인 도서 데이터를 타겟으로 함.
* 매일 "크롤링 날짜"별 "판매지수" 데이터 수집. 월별 데이터는 각각 분리된 테이블에 저장됨.
* 2022년 11월 도서 별 판매지수는 별도 테이블에 총계로 저장됨.
* 'product_analytics' 데이터베이스 내 '{yyyymm}math_book_analytics{본인영문이름}' 형태의 테이블에 데이터 저장.
* 상태 테이블은 매월 독립된 테이블 생성, 판매지수 평균은 'online_data' 테이블에 저장됨.
* transform 함수에 데이터 정상 추출 및 적재 검증 로직 적용.

## 📌 느낀점 
* 배치프로그램과 파이프라인에 대한 이해가 상승했으며, postgreSQL과 NoSQL 등 데이터베이스의 문법을 상당히 익힐 수 있었음
