package com.study.querydsl;


import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.study.querydsl.dto.MemberDto;
import com.study.querydsl.dto.QMemberDto;
import com.study.querydsl.dto.UserDto;
import com.study.querydsl.entity.Member;
import com.study.querydsl.entity.QMember;
import com.study.querydsl.entity.Team;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.select;
import static com.study.querydsl.entity.QMember.member;
import static com.study.querydsl.entity.QTeam.team;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory qFactory;

    @BeforeEach
    public void before() {
        qFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        //JPQL member1 찾기
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
        //Querydsl member1 찾기
        // 1. JPAQueryFactory 에 EntityManager(em)을 넘겨줘야한다.
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        QMember qMember = new QMember("m");
        // 1. Q클래스를 static import 하면  코드를 깔끔하게 작성할 수 있다.
        // * 컴파일 시점에서 오류를 발견해준다.
        Member findMember = queryFactory
                .select(qMember)
                .from(qMember)
                .where(qMember.username.eq("member1"))
                .fetchOne();
        // JPQL에서는 파라미터를 바인딩 해줘야하지만 Querydsl에서는 자동으로 처리한다.
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl2() {
        //Querydsl2 member1 찾기 (코드 간결하게 만들기)
        // 2. JPAQueryFactory는 필드에서 선언해줘도 된다.
//        qFactory = new JPAQueryFactory(em);


        // 1. Q클래스를 static import 하면  코드를 깔끔하게 작성할 수 있다.
        // * 컴파일 시점에서 오류를 발견해준다.
        Member findMember = qFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        // JPQL에서는 파라미터를 바인딩 해줘야하지만 Querydsl에서는 자동으로 처리한다.
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = qFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();
        /*
         * eq() // username = 'member1'
         * ne() // username != 'member1'
         * eq().not() // username != 'member1'
         * isNotNull() // username is not null
         * in(10,20) // age in(10,20)
         * notIn(10,20) // age not in(10, 20)
         * between(10,30) // between 10 ~ 30
         * goe(30) // >= 30
         * gt(30) // > 30
         * loe(30) // <= 30
         * lt(30) // < 30
         * like("string%") // like
         * contains("string") // like '%string%' 검색
         * startsWith("string") // like 'string%' 검색
         * */

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {
        Member findMember = qFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
        //멤버를 리스트로 조회
        List<Member> fetch = qFactory
                .selectFrom(member)
                .fetch();
        //멤버를 단건 조회
        Member fetchOne = qFactory
                .selectFrom(member)
                .fetchOne();
        // 처음 한 것 조회
        Member fetchFirst = qFactory
                .selectFrom(member)
                .fetchFirst();
        // 페이징에서 사용
        QueryResults<Member> results = qFactory
                .selectFrom(member)
                .fetchResults();

        results.getTotal();
        List<Member> content = results.getResults();
        //count 쿼리로 변경
        long total = qFactory
                .selectFrom(member)
                .fetchCount();

        /*
         * fetch() : 리스트 조회, 데이터 없으면 빈 리스트 반환
         * fetchOne() : 단 건 조회
         * -> 결과가 없다면 Null
         * -> 결과가 둘 이상이면 'com.querydsl.core.NonUniqueResultException' 에러 발생
         * fetchFirst() : 'limit(1).fetchOne()'
         * fetchResults() : 페이징 정보 포함, total count 쿼리 추가 실행
         * fetchCount() : count 쿼리로 변경해서 count 수 조회
         * */
    }


    /*
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc)
     * 단 2에서 회원 이름이 엇으면 마지막에 출력(nulls last)
     * */
    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));
        List<Member> result = qFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();

    }

    @Test
    public void paging1() {
        List<Member> result = qFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();
        assertThat(result.size()).isEqualTo(2);

    }

    @Test
    public void paging2() {
        QueryResults<Member> queryResults = qFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();
        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults().size()).isEqualTo(2);

    }

    @Test
    public void aggregation() {
        List<Tuple> result = qFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();
        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

    }

    /*
     * 팀 이름과 각 팀의 편균 연령을 구해라.
     */

    @Test
    public void group() {
        List<Tuple> result = qFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15); // (10 + 20) / 2

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35); // (30 + 40) / 2

    }

    /*
     * 팀 A에 소속된 모든 회원
     * */
    @Test
    public void join() {
        List<Member> result = qFactory
                .selectFrom(member)
                .join(member.team, team)
//                .leftJoin(member.team, team)

                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /*
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회*/
    @Test
    public void theta_join() {
        /*
         * from 절에 여러 엔티티를 선택해서 세타 조인
         * 외부 조인 불가능 -> on을 사용하면 외부 조인 가능*/
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = qFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result).extracting("username").containsExactly("teamA", "teamB");
    }

    /*
     * ex) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL : Select m, t from member m left join m.team t on t.name = 'teamA'
     * */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = qFactory.select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("Tuple : " + tuple);
        }
        // 그냥 inner join에서 on절을 사용하는 것은 그냥 where절에서 조건을 거나 똑같은 결과
    }


    /*
     * 연관관계 없는 엔티티를 외부 조인
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     * */
    @Test
    public void join_on_no_relation() {
        /*
         * from 절에 여러 엔티티를 선택해서 세타 조인
         * 외부 조인 불가능 -> on을 사용하면 외부 조인 가능*/
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = qFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("Tuple : " + tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetch_join_no() {
        em.flush();
        em.clear();

        Member findMember = qFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isFalse();

    }

    @Test
    public void fetch_join_use() {
        em.flush();
        em.clear();

        Member findMember = qFactory
                .selectFrom(member)
                .join(member.team, team)
                .fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isTrue();

    }

    /*
     * 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQueryEq() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = qFactory
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age").containsExactly(40);


    }

    /*
     * 나이가 평균보다 많은 회원들 조회
     */
    @Test
    public void subQueryGoe() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = qFactory
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age").containsExactly(30, 40);

    }

    @Test
    public void subQueryIn() {
        QMember memberSub = new QMember("memberSub");

        List<Member> result = qFactory
                .selectFrom(member)
                .where(member.age.in(
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        assertThat(result).extracting("age").containsExactly(20, 30, 40);

    }

    @Test
    public void selectSubQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = qFactory.select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("Tuple : " + result);
        }
    }

    @Test
    public void basicCase() {
        List<String> result = qFactory.select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s : " + s);
        }

    }

    @Test
    public void complexCase() {
        List<String> result = qFactory.select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s : " + s);
        }
    }

    @Test
    public void constant() {
        List<Tuple> result = qFactory.select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("Tuple : " + tuple);
        }
    }

    @Test
    public void concat() {
        List<String> result = qFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for (String s : result) {
            System.out.println("s : " + s);
        }
    }

    @Test
    public void simpleProjection() {
        List<String> result = qFactory
                .select(member.username)
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s : " + s);
        }
    }

    @Test
    public void tupleProjection() {
        // tuple 도 repository에서만 쓰고 나갈때는 dto로 바꾸기
        List<Tuple> result = qFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println("username : " + username);
            System.out.println("age : " + age);

        }

    }

    //DTO조회(JPQL)
    @Test
    public void findDtoByJPQL() {
        List<MemberDto> result = em.createQuery("select new com.study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();
        for (MemberDto memberDto : result) {
            System.out.println("memberDto : " + memberDto);
        }
    }
    // 순수 JPA에서 Dto를 조회할 때는 New 명령어를 사용해야함
    // Dto의 Package 이름을 다 적어줘야해서 지저분함
    // 생성자 방식만 지원한다.

    //DTO조회(setter)
    @Test
    public void findDtoBySetter() {
        List<MemberDto> result = qFactory
                .select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto : " + memberDto);
        }
    }

    //DTO조회(field)
    @Test
    public void findDtoByField() {
        List<MemberDto> result = qFactory
                .select(Projections.fields(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto : " + memberDto);
        }
    }

    //DTO조회(constructor)
    @Test
    public void findDtoByConstructor() {
        List<MemberDto> result = qFactory
                .select(Projections.constructor(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto : " + memberDto);
        }
    }

    //DTO조회(fields, userDto)
    @Test
    public void findUserDto() {
        List<UserDto> result = qFactory
                .select(Projections.fields(UserDto.class, member.username.as("name"), member.age))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("UserDto : " + userDto);
        }
    }

    //DTO조회(fields, userDto, SubQuery)
    @Test
    public void findSubUserDto() {
        QMember memberSub = new QMember("memberSub");
        List<UserDto> result = qFactory
                .select(Projections.fields(UserDto.class, member.username.as("name"),
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub), "age")
                ))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("UserDto : " + userDto);
        }
    }
    // 프로퍼티나, 필드 접근 생성 방식에서 이름이 다를 때 해결 방안
    // ExpressionUtils.as(source, alias) : 필드나, 서브 쿼리에 별칭 적용
    // 'username.as("memberName")' : 필드에 별칭 적용

    @Test
    public void findDtoByConstructor2() {
        List<UserDto> result = qFactory
                .select(Projections.constructor(UserDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("UserDto : " + userDto);
        }
    }

    @Test
    public void findDtoByQueryProjection() {
        List<MemberDto> result = qFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto : " + memberDto);
        }
    }
    // @QueryProjection의 단점은 DTO가 annotation에 의존해야한다.

    @Test
    public void dynamicQuery_BooleanBuilder() {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember1(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);

    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder();
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }
        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return qFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }


}
