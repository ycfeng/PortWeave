package dev.portweave.app.navigation

import androidx.compose.animation.AnimatedContentTransitionScope
import androidx.compose.animation.core.tween
import androidx.compose.runtime.Composable
import androidx.navigation.NavType
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.rememberNavController
import androidx.navigation.navArgument
import dev.portweave.app.AppContainer
import dev.portweave.app.feature.editor.ConfigEditorScreen
import dev.portweave.app.feature.home.HomeScreen
import dev.portweave.app.feature.detail.ConfigDetailScreen
import dev.portweave.app.feature.logs.LogScreen
import java.util.UUID
import dev.portweave.app.feature.settings.SettingsScreen

private const val HomeRoute = "home"
private const val NewConfigRoute = "editor/new"
private const val EditConfigRoute = "editor/{configId}"
private const val SettingsRoute = "settings"
private const val DetailRoute = "detail/{configId}"
private const val LogsRoute = "logs/{configId}"

@Composable
fun PortWeaveNavGraph(container: AppContainer) {
    val navController = rememberNavController()
    NavHost(
        navController = navController,
        startDestination = HomeRoute,
        enterTransition = {
            slideIntoContainer(
                towards = AnimatedContentTransitionScope.SlideDirection.Left,
                animationSpec = tween(NavigationAnimationDurationMillis),
            )
        },
        exitTransition = {
            slideOutOfContainer(
                towards = AnimatedContentTransitionScope.SlideDirection.Left,
                animationSpec = tween(NavigationAnimationDurationMillis),
            )
        },
        popEnterTransition = {
            slideIntoContainer(
                towards = AnimatedContentTransitionScope.SlideDirection.Right,
                animationSpec = tween(NavigationAnimationDurationMillis),
            )
        },
        popExitTransition = {
            slideOutOfContainer(
                towards = AnimatedContentTransitionScope.SlideDirection.Right,
                animationSpec = tween(NavigationAnimationDurationMillis),
            )
        },
    ) {
        composable(HomeRoute) {
            HomeScreen(
                repository = container.configRepository,
                runtimeStateStore = container.runtimeStateStore,
                onAdd = { navController.navigate(NewConfigRoute) },
                onEdit = { navController.navigate("detail/$it") },
                onSettings = { navController.navigate(SettingsRoute) },
            )
        }
        composable(
            route = DetailRoute,
            arguments = listOf(navArgument("configId") { type = NavType.StringType }),
        ) { entry ->
            val id = UUID.fromString(requireNotNull(entry.arguments?.getString("configId")))
            ConfigDetailScreen(
                configId = id,
                repository = container.configRepository,
                transfer = container.configTransferService,
                events = container.eventRepository,
                runtimeStore = container.runtimeStateStore,
                credentialStore = container.credentialStore,
                tunnelServiceClient = container.tunnelServiceClient,
                onBack = navController::popBackStack,
                onDeleted = navController::popBackStack,
                onEdit = { navController.navigate("editor/$id") },
                onLogs = { navController.navigate("logs/$id") },
            )
        }
        composable(
            route = LogsRoute,
            arguments = listOf(navArgument("configId") { type = NavType.StringType }),
        ) { entry ->
            val raw = entry.arguments?.getString("configId")
            LogScreen(
                events = container.eventRepository,
                configId = raw?.takeUnless { it == "all" }?.let(UUID::fromString),
                onBack = navController::popBackStack,
            )
        }
        composable(NewConfigRoute) {
            ConfigEditorScreen(
                repository = container.configRepository,
                transfer = container.configTransferService,
                credentialStore = container.credentialStore,
                sshBackend = container.sshTunnelBackend,
                tunnelServiceClient = container.tunnelServiceClient,
                configId = null,
                onBack = navController::popBackStack,
            )
        }
        composable(
            route = EditConfigRoute,
            arguments = listOf(navArgument("configId") { type = NavType.StringType }),
        ) { entry ->
            ConfigEditorScreen(
                repository = container.configRepository,
                transfer = container.configTransferService,
                credentialStore = container.credentialStore,
                sshBackend = container.sshTunnelBackend,
                tunnelServiceClient = container.tunnelServiceClient,
                configId = entry.arguments?.getString("configId"),
                onBack = navController::popBackStack,
            )
        }
        composable(SettingsRoute) {
            SettingsScreen(
                settingsStore = container.settingsStore,
                repository = container.configRepository,
                transfer = container.configTransferService,
                diagnostics = container.diagnosticBundleExporter,
                events = container.eventRepository,
                runtimeStore = container.runtimeStateStore,
                onLogs = { navController.navigate("logs/all") },
                onBack = navController::popBackStack,
            )
        }
    }
}

private const val NavigationAnimationDurationMillis = 300
