package com.vincenthzr.locationspoofer.viewmodel

import android.app.DownloadManager
import android.content.Context
import android.content.Intent
import android.os.Environment
import android.widget.Toast
import androidx.core.content.FileProvider
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.vincenthzr.locationspoofer.data.model.GithubRelease
import com.vincenthzr.locationspoofer.ui.R
import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray

data class UpdateUiState(
    val isLoading: Boolean = false,
    val error: String? = null,
    val releases: List<GithubRelease> = emptyList(),
    val activeDownloadId: Long? = null,
    val activeDownloadUrl: String? = null,
    val downloadProgress: Int = 0,
    val downloadStatus: Int = DownloadManager.STATUS_PENDING
)

class UpdateViewModel(private val context: Context) : ViewModel() {
    private val _uiState = MutableStateFlow(UpdateUiState())
    val uiState: StateFlow<UpdateUiState> = _uiState.asStateFlow()

    private val okHttpClient = OkHttpClient()

    /** 安装包几十 MB，读超时放宽，避免网速慢时中途断开 */
    private val downloadClient = okHttpClient.newBuilder()
        .readTimeout(60, TimeUnit.SECONDS)
        .build()

    fun fetchReleases() {
        _uiState.update { it.copy(isLoading = true, error = null) }
        viewModelScope.launch(Dispatchers.IO) {
            try {
                val request = Request.Builder()
                    .url("https://api.github.com/repos/liliwiki/LocationSpoofer/releases")
                    .header("Accept", "application/vnd.github.v3+json")
                    .build()

                val response = okHttpClient.newCall(request).execute()
                if (!response.isSuccessful) {
                    throw Exception("Failed to fetch updates: ${response.code}")
                }

                val jsonStr = response.body?.string() ?: "[]"
                val jsonArray = JSONArray(jsonStr)
                val releaseList = mutableListOf<GithubRelease>()

                for (i in 0 until jsonArray.length()) {
                    val obj = jsonArray.getJSONObject(i)
                    val tagName = obj.optString("tag_name", "Unknown")
                    val body = obj.optString("body", "")
                    val publishedAt = obj.optString("published_at", "")
                    val isPrerelease = obj.optBoolean("prerelease", false)

                    var downloadUrl: String? = null
                    var downloadUrl32Bit: String? = null
                    val assets = obj.optJSONArray("assets")
                    if (assets != null) {
                        for (j in 0 until assets.length()) {
                            val asset = assets.getJSONObject(j)
                            val assetName = asset.optString("name", "")
                            if (assetName.endsWith(".apk")) {
                                val url = asset.optString("browser_download_url")
                                if (assetName.contains("armeabi-v7a") || assetName.contains("32")) {
                                    downloadUrl32Bit = url
                                } else if (assetName.contains("arm64-v8a")) {
                                    downloadUrl = url
                                } else {
                                    // 通用安装包，如果没有专门的64位，就默认用通用包作为主链接
                                    if (downloadUrl == null) {
                                        downloadUrl = url
                                    }
                                }
                            }
                        }
                    }

                    if (downloadUrl == null && downloadUrl32Bit != null) {
                        downloadUrl = downloadUrl32Bit
                    }

                    releaseList.add(
                        GithubRelease(
                            tagName,
                            body,
                            downloadUrl,
                            downloadUrl32Bit,
                            publishedAt,
                            isPrerelease
                        )
                    )
                }

                withContext(Dispatchers.Main) {
                    _uiState.update { it.copy(isLoading = false, releases = releaseList) }
                }
            } catch (e: Exception) {
                withContext(Dispatchers.Main) {
                    _uiState.update { it.copy(isLoading = false, error = e.message) }
                }
            }
        }
    }

    private var downloadJob: Job? = null
    private var downloadedApk: File? = null

    /**
     * 用 App 自己的网络连接下载，不走系统下载管理器：
     * 部分 ROM（如 vivo）的下载管理器连 GitHub 会一直失败重试，界面停在 0% 且不报错。
     */
    fun startDownload(url: String, versionName: String) {
        downloadJob?.cancel()
        val dir = context.getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS) ?: context.cacheDir
        val target = File(dir, "LocationSpoofer_$versionName.apk")
        val partial = File(dir, target.name + ".part")
        downloadedApk = null
        _uiState.update {
            it.copy(
                activeDownloadId = System.currentTimeMillis(),
                activeDownloadUrl = url,
                downloadProgress = 0,
                downloadStatus = DownloadManager.STATUS_RUNNING
            )
        }
        downloadJob = viewModelScope.launch(Dispatchers.IO) {
            try {
                val request = Request.Builder().url(url).build()
                downloadClient.newCall(request).execute().use { response ->
                    if (!response.isSuccessful) throw IOException("HTTP ${response.code}")
                    val body = response.body ?: throw IOException("empty response")
                    val total = body.contentLength()
                    var done = 0L
                    var lastPercent = -1
                    body.byteStream().use { input ->
                        partial.outputStream().use { output ->
                            val buffer = ByteArray(64 * 1024)
                            while (true) {
                                ensureActive()
                                val n = input.read(buffer)
                                if (n < 0) break
                                output.write(buffer, 0, n)
                                done += n
                                val percent = if (total > 0) (done * 100 / total).toInt() else 0
                                if (percent != lastPercent) {
                                    lastPercent = percent
                                    _uiState.update { it.copy(downloadProgress = percent) }
                                }
                            }
                        }
                    }
                    if (total > 0 && done != total) throw IOException("incomplete download")
                }
                target.delete()
                if (!partial.renameTo(target)) throw IOException("cannot save the file")
                downloadedApk = target
                _uiState.update { it.copy(downloadProgress = 100, downloadStatus = DownloadManager.STATUS_SUCCESSFUL) }
            } catch (e: CancellationException) {
                partial.delete()
                throw e
            } catch (e: Exception) {
                partial.delete()
                _uiState.update {
                    it.copy(
                        activeDownloadId = null,
                        activeDownloadUrl = null,
                        downloadProgress = 0,
                        downloadStatus = DownloadManager.STATUS_FAILED
                    )
                }
                withContext(Dispatchers.Main) {
                    Toast.makeText(
                        context,
                        context.getString(R.string.update_download_failed, e.message ?: e.javaClass.simpleName),
                        Toast.LENGTH_LONG
                    ).show()
                }
            }
        }
    }

    fun installApk() {
        val apk = downloadedApk?.takeIf { it.exists() } ?: return
        val uri = FileProvider.getUriForFile(context, "${context.packageName}.updateprovider", apk)
        val intent = Intent(Intent.ACTION_VIEW).apply {
            setDataAndType(uri, "application/vnd.android.package-archive")
            flags = Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_GRANT_READ_URI_PERMISSION
        }
        context.startActivity(intent)
    }

    fun cancelDownload() {
        downloadJob?.cancel()
        downloadJob = null
        downloadedApk = null
        _uiState.update {
            it.copy(
                activeDownloadId = null,
                activeDownloadUrl = null,
                downloadProgress = 0,
                downloadStatus = DownloadManager.STATUS_PENDING
            )
        }
    }
}
