# MovieRatings-spark

[PySpark - Movie Rating](https://common-fang-be4.notion.site/PySpark-Movie-Rating-bfa14ad53aa04534920b0591bce4043e?pvs=4)

## 데이터브릭스 클라우드에서 실행하기

데이터브릭스 환경에서 예제를 실행하려면 두 가지 단계를 거쳐야합니다.

1. [데이터브릭스 사이트](https://www.databricks.com/try-databricks)에 가입합니다.
2. 실행을 위해 개별 노트북 파일을 임포트합니다.

데이터브릭스는 관리형 클라우드이므로 다음과 같은 기능을 지원합니다.

- 관리형 스파크 클러스터 환경
- 대화형 데이터 탐색 및 시각화 기능
- 운영용 파이프라인 스케줄러
- 선호하는 스파크 기반 애플리케이션을 위한 플랫폼

### 노트북 임포트 과정

1. 임포트하려는 노트북 파일을 결정합니다.
   예를들어, [파이썬 버전의 3장 예제](https://github.com/databricks/Spark-The-Definitive-Guide/blob/master/code/A_Gentle_Introduction_to_Spark-Chapter_3_A_Tour_of_Sparks_Toolset.py)
   에 접속합니다. 그리고 파일보기 방식중 RAW 형태로 보기 버튼을 선택하여 데스크탑에 저장합니다. 다른 방법으로 git 명령을 이용해 이
   코드 저장소를 모두 로컬로 복제할 수도 있습니다.
2. 데이터브릭스 환경에 파일을 업로드합니다 노트북 임포트 하는 방법을
   소개하는 [이 URL](https://docs.databricks.com/en/notebooks/index.html#import-a-notebook)을 숙지합니다. 데이터브릭스 워크스페이스를 열고 임포트 대상
   파일 경로로 이동합니다. 거기서 업로드할 파일을 선택합니다.
   아쉽게도, 최근에 강화된 보안 정책에 따라 외부 URL에서 노트북 파일을 임포트 할 수 없습니다. 따라서, 반드시 로컬에서 파일을 업로드해야 합니다.
3. 준비가 거의 끝났습니다. 이제 노트북을 실행하기만 하면 됩니다. 모든 예제는 데이터브릭스 런타임 3.1 버전 이상에서 실행할 수 있습니다. 따라서 클러스터를 생성할 때 버전을 3.1이상으로 지정해야 합니다.
   클러스터를 생성하고 나면 노트북에 연결할 수 있습니다.
    4. 각 노트북의 예제 데이터 경로를 변경합니다. 모든 예제 데이터를 직접 업로드하지 마시고 각 장의 예제에 등장하는 `/data`
       를 `/databricks-datasets/definitive-guide/data`로 변경해
       사용하는 것이 좋습니다. 경로를 변경하고 나면 모든 예제가 큰 문제 없이 실행됩니다. "find"와 "replace" 기능을 이용하면 이 과정을 단순하게 처리할 수 있습니다.
