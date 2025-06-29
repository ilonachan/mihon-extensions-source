package eu.kanade.tachiyomi.extension.all.pixiv

import eu.kanade.tachiyomi.source.model.FilterList
import eu.kanade.tachiyomi.source.model.MangasPage
import eu.kanade.tachiyomi.source.model.Page
import eu.kanade.tachiyomi.source.model.SChapter
import eu.kanade.tachiyomi.source.model.SManga
import eu.kanade.tachiyomi.source.online.HttpSource
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromJsonElement
import okhttp3.Headers
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.Request
import okhttp3.Response
import rx.Observable
import uy.kohesive.injekt.injectLazy
import java.util.Collections

@Serializable
internal class Optional<T> private constructor(private val value: T?) {
    companion object {
        fun <T> of(o: T) = Optional(o)
        fun <T> ofNullable(o: T?) = Optional(o)
        fun <T> empty() = Optional<T>(null)
    }

    fun get(): T = value!!
    fun getOrNull(): T? = value
}

class Pixiv(override val lang: String) : HttpSource() {
    override val name = "Pixiv"
    override val baseUrl = "https://www.pixiv.net"
    override val supportsLatest = true

    private val json: Json by injectLazy()

    override fun headersBuilder(): Headers.Builder =
        super.headersBuilder().add("Referer", "$baseUrl/")

    private open inner class HttpCall(href: String?) {
        val url: HttpUrl.Builder = baseUrl.toHttpUrl()
            .run { href?.let { newBuilder(it)!! } ?: newBuilder() }

        val request: Request.Builder = Request.Builder()
            .headers(headersBuilder().build())

        fun execute(): Response =
            client.newCall(request.url(url.build()).build()).execute()
    }

    class PixivApiException(message: String? = null) : Exception(message, null)

    private inner class ApiCall(href: String?) : HttpCall(href) {
        init {
            url.addEncodedQueryParameter("lang", lang)
            request.addHeader("Accept", "application/json")
        }

        /**
         * Sends the previously constructed API call to the Pixiv API.
         * If the server reports an error, A [PixivApiException] will be
         * returned as a [Result.failure].
         */
        inline fun <reified T> executeApi(): Result<T> {
            val resp = json.decodeFromString<PixivApiResponse>(execute().body.string())
            if (resp.error) {
                return Result.failure(PixivApiException(resp.message))
            }
            return Result.success(json.decodeFromJsonElement<T>(resp.body!!))
        }
    }

    /**
     * Detail information for artworks, either "illustrations" or "manga" chapters
     */
    private val IllustDetails = object {
        /**
         * The maximum amount of IDs that the server will accept in a [fetchMany] request.
         * Anything beyond that still needs to be chunked into smaller requests.
         */
        val FETCH_MANY_LIMIT = 100
        val cache by lazy { lruCache<String, Optional<PixivIllust>>(100) { Optional.ofNullable(fetch(it)) } }

        /**
         * Fetches detail information about the given illustration ID.
         *
         * **NOTE:** consider using [get], which supports caching.
         */
        fun fetch(id: String): PixivIllust? =
            ApiCall("/touch/ajax/illust/details").apply {
                url.addEncodedQueryParameter("illust_id", id)
            }.executeApi<PixivIllustDetails>().getOrNull()?.illust_details.also {
                cache.put(id, Optional.ofNullable(it))
            }

        /**
         * Fetches detail information about many given illustration IDs at once.
         * Note that if the list size exceeds [FETCH_MANY_LIMIT], it still needs to be
         * split into multiple requests; so this function can't guarantee only one request
         * will be used in all cases.
         *
         * **NOTE:** consider using [getMany], which supports caching (and that cache
         * is shared with previous [fetch]/[get] calls)
         */
        fun fetchMany(ids: Iterable<String>) =
            ids.chunked(FETCH_MANY_LIMIT).flatMap {
                ApiCall("/touch/ajax/illust/details/many").apply {
                    ids.forEach {
                        url.addEncodedQueryParameter("illust_ids[]", it)
                    }
                }.executeApi<PixivIllustsDetails>().getOrThrow().illust_details!!
            }.map {
                cache.put(it.id, Optional.ofNullable(it))
                it
            }

        /**
         * Retrieves detail information about the given illustration ID,
         * either from the local [LruCache][cache] or from the API using [fetch].
         */
        fun get(id: String): PixivIllust? = cache.get(id).getOrNull()

        /**
         * Retrieves detail information about many given illustration IDs at once.
         * Elements are loaded from the local [LruCache][cache] if available, and
         * the remainder is bulk-fetched using [fetchMany].
         */
        fun getMany(ids: Iterable<String>): List<PixivIllust> {
            val snap = cache.snapshot().keys
            val (found, toFetch) = ids.partition { snap.contains(it) }
            return found.asSequence().map { get(it) }.filterNotNull()
                .plus(if (toFetch.isNotEmpty()) fetchMany(toFetch) else emptyList()).toList()
        }
    }

    private val IllustPages = object {
        val cache by lazy { lruCache(10, ::fetch) }

        fun fetch(id: String): List<PixivIllustPageUrls?>? =
            ApiCall("/ajax/illust/$id/pages")
                .executeApi<List<PixivIllustPage>>()
                .getOrNull()?.map { it.urls }.also {
                    cache.put(id, it)
                }

        fun get(id: String) = cache.get(id)
    }

    override fun fetchPageList(chapter: SChapter): Observable<List<Page>> {
        val illustId = chapter.url.substringAfterLast('/')

        val pages = IllustPages.get(illustId)!!
            .mapIndexed { i, it -> Page(i, chapter.url, it?.original) }

        return Observable.just(pages)
    }

    private val SeriesDetails = object {
        val cache by lazy { lruCache<String, Optional<PixivSeries>>(25) { Optional.ofNullable(fetch(it)) } }

        /**
         * Fetches detail information about the given series ID.
         *
         * **NOTE:** consider using [get], which supports caching.
         */
        fun fetch(id: String): PixivSeries? =
            ApiCall("/touch/ajax/illust/series/$id")
                .executeApi<PixivSeriesDetails>().getOrNull()?.series.also {
                    cache.put(id, if (it != null) Optional.of(it) else Optional.empty())
                }

        /**
         * Retrieves detail information about the given series ID,
         * either from the local [LruCache][cache] or from the API using [fetch].
         */
        fun get(id: String): PixivSeries? = cache.get(id).getOrNull()

        val illustsCache by lazy { lruCache(25, ::fetchIllusts) }

        /**
         * Fetches the list of all illustrations belonging to the specified series ID.
         *
         * **NOTE:** consider using [getIllusts], which supports caching.
         *
         * @return the list of illustrations (potentially empty), or `null` if the series doesn't exist
         */
        fun fetchIllusts(id: String): List<PixivIllust>? {
            var startingAt = 0
            val call = ApiCall("/touch/ajax/illust/series_content/$id")

            // This is awful. I would have loved to use buildList, but it seems there's no way to abort that
            // (which I need to do if the series doesn't exist
            var list: MutableList<PixivIllust>? = null
            while (true) {
                val illusts = call.apply {
                    url.setEncodedQueryParameter("last_order", startingAt.toString())
                }.executeApi<PixivSeriesContents>()
                    // the API call should only fail if the series doesn't exist; we return null then
                    .getOrNull()?.series_contents ?: break

                // At this point we know the series exist, so the list should be non-null.
                if (list == null) {
                    list = mutableListOf()
                }

                // Once we encounter an empty page, stop collecting.
                if (illusts.isEmpty()) break

                list.addAll(illusts)
                startingAt += illusts.size
            }
            return list?.let { Collections.unmodifiableList(it) }
                .also { l ->
                    illustsCache.put(id, l)
                }
        }

        /**
         * Retrieves the list of all illustrations belonging to the specified series ID,
         * either from the local [LruCache][illustsCache] or from the API using [fetchIllusts].
         *
         * @return the list of illustrations (potentially empty), or `null` if the series doesn't exist
         */
        fun getIllusts(id: String): List<PixivIllust>? = illustsCache.get(id)
    }

    /**
     * Pixiv ranks each day's top 500 manga, this is a good source for the "Popular" tab
     */
    private val PopularRanking = object {
        /**
         * Fetches the ranked illustration IDs page by page.
         *
         * The page size seems to be 18, though we make no assumptions about this.
         * Using the server page size has the advantage that we can give Tachiyomi more
         * freedom to fetch pages in any order as needed, which improves performance and UX.
         */
        fun fetchIdsServerPage(page: Int = 1) =
            ApiCall("/touch/ajax/ranking/illust?mode=daily&type=manga").apply {
                url.setEncodedQueryParameter("page", page.toString())
            }.executeApi<PixivRankings>().getOrNull()?.ranking?.map { it.illustId!! }
                ?: emptyList()

        /**
         * Fetches detail data for all ranked illustrations page by page. Essentially just combines
         * [fetchIdsServerPage] with [IllustDetails.getMany][IllustDetails].
         */
        fun fetchServerPage(page: Int = 1) = IllustDetails.getMany(fetchIdsServerPage(page))

        /**
         * Fetches all ranked illustration IDs in order, by simply calling [fetchIdsServerPage]
         * as needed until there are no pages left, yielding the result.
         */
        fun streamIds() = sequence {
            for (p in countUp(start = 1)) {
                val ids = fetchIdsServerPage(p)
                if (ids.isEmpty()) return@sequence
                yieldAll(ids)
            }
        }

        /**
         * Fetches detail data for all ranked illustrations in order, by calling [fetchServerPage]
         * as needed until there are no pages left, yielding the result.
         */
        fun stream() = sequence {
            for (p in countUp(start = 1)) {
                yieldAll(fetchServerPage(p).apply { if (isEmpty()) return@sequence })
            }
        }
    }

    /**
     * TODO: maybe use the prebuilt Request/Response mechanism instead (is there any advantage?)
     */
    override fun fetchPopularManga(page: Int): Observable<MangasPage> {
        val mangas = PopularRanking.fetchServerPage(page).toSManga()
        return Observable.just(MangasPage(mangas, hasNextPage = mangas.isNotEmpty()))
    }

    private val LatestPage = object {
        fun fetchServerPage(page: Int = 1) =
            ApiCall("/touch/ajax/latest?type=manga").apply {
                url.setEncodedQueryParameter("p", page.toString())
            }.executeApi<PixivResults>().getOrNull()?.illusts?.filterNot { it.is_ad_container == 1 }
                ?: emptyList()

        fun stream() = sequence {
            for (p in countUp(start = 1)) {
                yieldAll(fetchServerPage(p).apply { if (isEmpty()) return@sequence })
            }
        }
    }

    override fun fetchLatestUpdates(page: Int): Observable<MangasPage> {
        val mangas = LatestPage.fetchServerPage(page).toSManga()
        return Observable.just(MangasPage(mangas, hasNextPage = mangas.isNotEmpty()))
    }

    data class BasicSearchParams(
        val word: String,
        val sMode: String,
        val order: String? = null,
        val mode: String? = null,
        val type: String? = null,
        val dateBefore: String? = null,
        val dateAfter: String? = null,
    )

    private fun BasicSearchParams.toApiCall() = ApiCall("/touch/ajax/search/illusts")
        .also { call ->
            call.url.addQueryParameter("word", word)
            call.url.addEncodedQueryParameter("s_mode", sMode)
            type?.let { call.url.addEncodedQueryParameter("type", it) }
            order?.let { call.url.addEncodedQueryParameter("order", it) }
            mode?.let { call.url.addEncodedQueryParameter("mode", it) }
            dateBefore?.let { call.url.addEncodedQueryParameter("ecd", it) }
            dateAfter?.let { call.url.addEncodedQueryParameter("scd", it) }
        }

    private val Search = object {
        val basicCallCache = lruCache<BasicSearchParams, ApiCall>(5) { it.toApiCall() }

        fun fetchBasicPage(params: BasicSearchParams, page: Int = 1) =
            basicCallCache.get(params)
                .apply {
                    url.setEncodedQueryParameter("p", page.toString())
                }.executeApi<PixivResults>().getOrThrow().illusts!!
                .filterNot { it.is_ad_container == 1 || it.type == "2" }

        fun streamBasic(params: BasicSearchParams) = sequence {
            for (p in countUp(start = 1)) {
                yieldAll(fetchBasicPage(params, p).apply { if (isEmpty()) return@sequence })
            }
        }

        fun fetchForUserPage(userId: String, type: String?, page: Int = 1) =
            ApiCall("/touch/ajax/user/illusts")
                .apply {
                    type?.let { url.setEncodedQueryParameter("type", it) }
                    url.setEncodedQueryParameter("id", userId)
                    url.setEncodedQueryParameter("p", page.toString())
                }.executeApi<PixivResults>().getOrThrow().illusts!!

        fun streamForUser(userId: String, type: String?) = sequence {
            for (p in countUp(start = 1)) {
                yieldAll(fetchForUserPage(userId, type, p).apply { if (isEmpty()) return@sequence })
            }
        }

        fun fetchUserIdsByName(nick: String, page: Int) =
            ApiCall("/ajax/search/users")
                .apply {
//                url.setQueryParameter("i", "0") // also lists users that aren't "creators"
                    url.setQueryParameter("nick", nick)
                    url.setEncodedQueryParameter("p", page.toString())
                }.executeApi<PixivUserSearchResponse>().getOrThrow().page.userIds

        fun streamUserIdsByName(nick: String) = sequence {
            for (p in countUp(start = 1)) {
                yieldAll(fetchUserIdsByName(nick, p).apply { if (isEmpty()) return@sequence })
            }
        }

        fun streamForUserByName(userNick: String, type: String?) = sequence {
            for (uid in streamUserIdsByName(userNick)) {
                yieldAll(streamForUser(uid.toString(), type))
            }
        }
    }

    private var searchNextPage = 1
    private var searchHash: Int? = null
    private lateinit var searchIterator: Iterator<SManga>

    override fun fetchSearchManga(
        page: Int,
        query: String,
        filters: FilterList,
    ): Observable<MangasPage> {
        val target = PixivTarget.fromUri(query) /*?: PixivTarget.fromSearchQuery(query)*/

        val singleResult = { manga: SManga? ->
            Observable.just(
                MangasPage(
                    if (manga != null) {
                        listOf(manga)
                    } else {
                        emptyList()
                    },
                    hasNextPage = false,
                ),
            )
        }

        // Deeplink selection of specific IDs: simply fetch the single object and return
        when (target) {
            is PixivTarget.Illustration ->
                singleResult(IllustDetails.get(target.illustId)?.toSManga())
            is PixivTarget.Series -> {
                singleResult(SeriesDetails.get(target.seriesId)?.toSManga())
            }
            else -> null
        }?.let { return it }

        val filters = filters.list as PixivFilters
        val hash = Pair(query, filters.toList()).hashCode()

        if (hash != searchHash || page == 1) {
            searchHash = hash

            lateinit var searchSequence: Sequence<PixivIllust>
            lateinit var predicates: List<(PixivIllust) -> Boolean>

            // TODO: it would be useful to allow multiple user: tags in the query
            if (target is PixivTarget.User) {
                searchSequence = Search.streamForUser(
                    userId = target.userId,
                    type = filters.type,
                )

                predicates = buildList {
                    filters.makeTagsPredicate()?.let(::add)
                    filters.makeRatingPredicate()?.let(::add)
                }
            } else if (query.isNotBlank()) {
                searchSequence = Search.streamBasic(
                    BasicSearchParams(
                        word = query,
                        order = filters.order,
                        mode = filters.rating,
                        sMode = "s_tc",
                        type = filters.type,
                        dateBefore = filters.dateBefore.ifBlank { null },
                        dateAfter = filters.dateAfter.ifBlank { null },
                    ),
                )

                predicates = buildList {
                    filters.makeTagsPredicate()?.let(::add)
                    filters.makeUsersPredicate()?.let(::add)
                }
            } else if (filters.users.isNotBlank()) {
                searchSequence = Search.streamForUserByName(
                    userNick = filters.users,
                    type = filters.type,
                )

                predicates = buildList {
                    filters.makeTagsPredicate()?.let(::add)
                    filters.makeRatingPredicate()?.let(::add)
                }
            } else {
                searchSequence = Search.streamBasic(
                    BasicSearchParams(
                        word = filters.tags.ifBlank { "漫画" },
                        order = filters.order,
                        mode = filters.rating,
                        sMode = filters.searchMode,
                        type = filters.type,
                        dateBefore = filters.dateBefore.ifBlank { null },
                        dateAfter = filters.dateAfter.ifBlank { null },
                    ),
                )

                predicates = emptyList()
            }

            if (predicates.isNotEmpty()) {
                searchSequence = searchSequence.filter { predicates.all { p -> p(it) } }
            }

            searchIterator = searchSequence.toSManga().iterator()
            searchNextPage = 2
        } else {
            require(page == searchNextPage++)
        }

        val mangas = searchIterator.truncateToList(50).toList()
        return Observable.just(MangasPage(mangas, hasNextPage = mangas.isNotEmpty()))
    }

    override fun getFilterList() = FilterList(PixivFilters())

    private fun List<PixivIllust>.toSManga() = asSequence().toSManga().toList()
    private fun Sequence<PixivIllust>.toSManga() = sequence {
        val seriesIdsSeen = mutableSetOf<String>()

        forEach { illust ->
            val manga = illust.toSManga()
            if (seriesIdsSeen.add(manga.url)) {
                yield(manga)
            }
        }
    }

    private fun PixivIllust.toSManga(): SManga {
        if (series == null) {
            val manga = SManga.create()
            manga.setUrlWithoutDomain("/artworks/${id!!}")
            manga.title = title ?: "(null)"
            manga.thumbnail_url = url
            return manga
        } else {
            val series = series.copy(userId = series.userId ?: author_details?.user_id)
            val manga = series.toSManga().apply {
                thumbnail_url = thumbnail_url ?: this@toSManga.url
            }
            return manga
        }
    }

    private fun PixivSeries.toSManga() = toSearchResult().toSManga()
    private fun PixivSearchResultSeries.toSManga(): SManga {
        val manga = SManga.create()
        manga.setUrlWithoutDomain("/user/${userId!!}/series/$id")
        manga.title = title ?: "(null)"
        manga.thumbnail_url = coverImage
        return manga
    }

    private fun PixivSeries.toSearchResult() = PixivSearchResultSeries(
        id = id,
        title = title,
        userId = userId,
        coverImage = coverImage?.let { if (it.isString) it.content else null },
    )

    override fun fetchMangaDetails(manga: SManga): Observable<SManga> {
        val (id, isSeries) = parseSMangaUrl(manga.url)

        if (isSeries) {
            val series = SeriesDetails.get(id)!!

            val illusts = SeriesDetails.getIllusts(id)!!

            if (series.id != null && series.userId != null) {
                manga.setUrlWithoutDomain("/user/${series.userId}/series/${series.id}")
            }

            series.title?.let { manga.title = it }
            series.caption?.let { manga.description = it }

            illusts.firstOrNull()?.author_details?.user_name?.let {
                manga.artist = it
                manga.author = it
            }

            val tags = illusts.flatMap { it.tags ?: emptyList() }.toSet()
            if (tags.isNotEmpty()) manga.genre = tags.joinToString()

            val coverImage = series.coverImage?.let { if (it.isString) it.content else null }
            (coverImage ?: illusts.firstOrNull()?.url)?.let { manga.thumbnail_url = it }
        } else {
            val illust = IllustDetails.get(id)!!

            illust.id?.let { manga.setUrlWithoutDomain("/artworks/$it") }
            illust.title?.let { manga.title = it }

            illust.author_details?.user_name?.let {
                manga.artist = it
                manga.author = it
            }

            illust.comment?.let { manga.description = it }
            illust.tags?.let { manga.genre = it.joinToString() }
            illust.url?.let { manga.thumbnail_url = it }
        }

        return Observable.just(manga)
    }

    override fun fetchChapterList(manga: SManga): Observable<List<SChapter>> {
        val (id, isSeries) = parseSMangaUrl(manga.url)

        val illusts = when (isSeries) {
            true -> SeriesDetails.getIllusts(id)!!
            false -> listOf(IllustDetails.get(id)!!)
        }

        val chapters = illusts.mapIndexed { i, illust ->
            SChapter.create().apply {
                setUrlWithoutDomain("/artworks/${illust.id!!}")
                name = illust.title ?: "(null)"
                date_upload = (illust.upload_timestamp ?: 0) * 1000
                chapter_number = (illusts.size - i).toFloat()
            }
        }

        return Observable.just(chapters)
    }

    override fun chapterListParse(response: Response): List<SChapter> =
        throw UnsupportedOperationException()

    override fun imageUrlParse(response: Response): String =
        throw UnsupportedOperationException()

    override fun latestUpdatesParse(response: Response): MangasPage =
        throw UnsupportedOperationException()

    override fun latestUpdatesRequest(page: Int): Request =
        throw UnsupportedOperationException()

    override fun mangaDetailsParse(response: Response): SManga =
        throw UnsupportedOperationException()

    override fun pageListParse(response: Response): List<Page> =
        throw UnsupportedOperationException()

    override fun popularMangaParse(response: Response): MangasPage =
        throw UnsupportedOperationException()

    override fun popularMangaRequest(page: Int): Request =
        throw UnsupportedOperationException()

    override fun searchMangaParse(response: Response): MangasPage =
        throw UnsupportedOperationException()

    override fun searchMangaRequest(page: Int, query: String, filters: FilterList): Request =
        throw UnsupportedOperationException()
}
