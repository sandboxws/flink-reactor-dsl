import type { ResolvedJar } from './connector-resolver.js';
import type { FlinkDeploymentCrd } from './crd-generator.js';

// ── Types ───────────────────────────────────────────────────────────

export type DeliveryStrategy = 'init-container' | 'custom-image';

/** Init-container configuration applied to the CRD */
export interface InitContainerPatch {
  readonly initContainers: readonly InitContainer[];
  readonly volumes: readonly Volume[];
  readonly volumeMounts: readonly VolumeMount[];
  readonly classpaths: readonly string[];
}

interface InitContainer {
  readonly name: string;
  readonly image: string;
  readonly command: readonly string[];
  readonly volumeMounts: readonly VolumeMount[];
}

interface Volume {
  readonly name: string;
  readonly emptyDir: Record<string, never>;
}

interface VolumeMount {
  readonly name: string;
  readonly mountPath: string;
}

/** Custom-image Dockerfile output */
export interface CustomImageResult {
  readonly dockerfile: string;
}

/** Delivery strategy options */
export interface DeliveryOptions {
  readonly strategy: DeliveryStrategy;
  /** Maven mirror URL for air-gapped environments */
  readonly mavenMirror?: string;
  /** Private container registry (e.g., 'registry.internal.com') */
  readonly imageRegistry?: string;
  /** Kubernetes imagePullSecret name */
  readonly imagePullSecret?: string;
}

// ── Constants ───────────────────────────────────────────────────────

const USRLIB_PATH = '/opt/flink/usrlib';
const VOLUME_NAME = 'flink-usrlib';

// ── Init-Container Strategy ─────────────────────────────────────────

/**
 * Generate init-container patches for the FlinkDeployment CRD.
 *
 * Creates a busybox init container that runs wget for each resolved JAR,
 * downloading them into a shared emptyDir volume mounted at /opt/flink/usrlib/.
 */
export function generateInitContainerPatch(
  jars: readonly ResolvedJar[],
): InitContainerPatch {
  if (jars.length === 0) {
    return {
      initContainers: [],
      volumes: [],
      volumeMounts: [],
      classpaths: [],
    };
  }

  // Build wget commands: one per JAR
  const wgetCommands = jars.map(
    (jar) => `wget -O ${USRLIB_PATH}/${jar.jarName} '${jar.downloadUrl}'`,
  );

  const initContainer: InitContainer = {
    name: 'fetch-connectors',
    image: 'busybox:1.36',
    command: ['sh', '-c', wgetCommands.join(' && ')],
    volumeMounts: [{ name: VOLUME_NAME, mountPath: USRLIB_PATH }],
  };

  const volume: Volume = {
    name: VOLUME_NAME,
    emptyDir: {},
  };

  const volumeMount: VolumeMount = {
    name: VOLUME_NAME,
    mountPath: USRLIB_PATH,
  };

  const classpaths = jars.map(
    (jar) => `file://${USRLIB_PATH}/${jar.jarName}`,
  );

  return {
    initContainers: [initContainer],
    volumes: [volume],
    volumeMounts: [volumeMount],
    classpaths,
  };
}

/**
 * Apply the init-container patch to a FlinkDeployment CRD object.
 * Adds initContainers, volumes, volumeMounts to podTemplate,
 * and pipeline.classpaths to flinkConfiguration.
 */
export function applyInitContainerPatch(
  crd: FlinkDeploymentCrd,
  patch: InitContainerPatch,
): FlinkDeploymentCrd {
  if (patch.initContainers.length === 0) return crd;

  const flinkConfig = { ...crd.spec.flinkConfiguration };
  if (patch.classpaths.length > 0) {
    flinkConfig['pipeline.classpaths'] = patch.classpaths.join(';');
  }

  return {
    ...crd,
    spec: {
      ...crd.spec,
      flinkConfiguration: flinkConfig,
      podTemplate: {
        spec: {
          initContainers: [...patch.initContainers],
          volumes: [...patch.volumes],
          containers: [
            {
              name: 'flink-main-container',
              volumeMounts: [...patch.volumeMounts],
            },
          ],
        },
      },
    },
  };
}

// ── Custom-Image Strategy ───────────────────────────────────────────

/**
 * Generate a Dockerfile that bakes resolved JARs into the Flink image.
 * Each JAR is ADDed from its Maven download URL to /opt/flink/usrlib/.
 */
export function generateDockerfile(
  jars: readonly ResolvedJar[],
  baseImage: string,
): CustomImageResult {
  const lines: string[] = [
    `FROM ${baseImage}`,
    '',
  ];

  for (const jar of jars) {
    lines.push(`ADD ${jar.downloadUrl} ${USRLIB_PATH}/${jar.jarName}`);
  }

  // Ensure usrlib is readable
  if (jars.length > 0) {
    lines.push('');
    lines.push(`RUN chmod -R 644 ${USRLIB_PATH}/*.jar`);
  }

  lines.push('');

  return { dockerfile: lines.join('\n') };
}

// ── Air-gapped Helpers ──────────────────────────────────────────────

const DEFAULT_MAVEN_BASE = 'https://repo1.maven.org/maven2';

/**
 * Substitute Maven Central URLs with a mirror URL.
 * Replaces the base URL in all resolved JAR download URLs.
 */
export function applyMavenMirror(
  jars: readonly ResolvedJar[],
  mirrorUrl: string,
): readonly ResolvedJar[] {
  return jars.map((jar) => ({
    ...jar,
    downloadUrl: jar.downloadUrl.replace(DEFAULT_MAVEN_BASE, mirrorUrl),
  }));
}

/**
 * Apply private registry configuration to a CRD.
 * Replaces the image with a registry-prefixed version and adds imagePullSecrets.
 */
export function applyPrivateRegistry(
  crd: FlinkDeploymentCrd,
  imageRegistry: string,
  imagePullSecret?: string,
): FlinkDeploymentCrd {
  const originalImage = crd.spec.image;
  // Prefix with registry unless already prefixed
  const prefixedImage = originalImage.includes('/')
    ? `${imageRegistry}/${originalImage}`
    : `${imageRegistry}/${originalImage}`;

  const result: FlinkDeploymentCrd = {
    ...crd,
    spec: {
      ...crd.spec,
      image: prefixedImage,
      ...(imagePullSecret
        ? {
            podTemplate: {
              ...(crd.spec.podTemplate as Record<string, unknown> | undefined),
              spec: {
                ...((crd.spec.podTemplate as Record<string, unknown> | undefined)?.spec as Record<string, unknown> | undefined),
                imagePullSecrets: [{ name: imagePullSecret }],
              },
            },
          }
        : {}),
    },
  };

  return result;
}
