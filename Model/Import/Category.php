<?php

namespace Magenest\ImportCategory\Model\Import;
use Exception;
use Magento\Catalog\Api\CategoryRepositoryInterface;
use Magento\Catalog\Model\CategoryFactory;
use Magento\Framework\App\ResourceConnection;
use Magento\Framework\Json\Helper\Data as JsonHelper;
use Magento\ImportExport\Helper\Data as ImportHelper;
use Magento\ImportExport\Model\Import;
use Magento\ImportExport\Model\Import\Entity\AbstractEntity;
use Magento\ImportExport\Model\Import\ErrorProcessing\ProcessingErrorAggregatorInterface;
use Magento\ImportExport\Model\ResourceModel\Helper;
use Magento\ImportExport\Model\ResourceModel\Import\Data;

class Category extends AbstractEntity
{
    const ENTITY_CODE = 'category';
    const TABLE = 'catalog_category_entity';
    const ENTITY_ID_COLUMN = 'entity_id';
    const STORE_ID = "store_id";
    const NAME = "name";
    const URL_KEY = "url_key";
    const PARENT = "parent";
    const IS_ACTIVE = "is_active";
    const INCLUDE_IN_MENU = "include_in_menu";
    const AVAILABLE_SORT_BY = "available_sort_by";

    const POSITION = "position";
    protected $needColumnCheck = true;
    protected $logInHistory = true;
    protected $validColumnNames = [
        self::ENTITY_ID_COLUMN,
        self::STORE_ID,
        self::PARENT,
        self::IS_ACTIVE,
        self::INCLUDE_IN_MENU,
        self::NAME,
        self::AVAILABLE_SORT_BY,
        self::URL_KEY,
        self::POSITION,
    ];
    protected $connection;
    private $resource;
    protected $modelCategoryFactory;
    protected $categoryRepository;
    public function __construct(
        JsonHelper $jsonHelper,
        ImportHelper $importExportData,
        Data $importData,
        ResourceConnection $resource,
        Helper $resourceHelper,
        ProcessingErrorAggregatorInterface $errorAggregator,
        CategoryFactory $modelCategoryFactory,
        CategoryRepositoryInterface $categoryRepository
    ) {
        $this->jsonHelper = $jsonHelper;
        $this->_importExportData = $importExportData;
        $this->_resourceHelper = $resourceHelper;
        $this->_dataSourceModel = $importData;
        $this->resource = $resource;
        $this->connection = $resource->getConnection(ResourceConnection::DEFAULT_CONNECTION);
        $this->errorAggregator = $errorAggregator;
        $this->modelCategoryFactory = $modelCategoryFactory;
        $this->initMessageTemplates();
        $this->categoryRepository = $categoryRepository;
    }
    public function getEntityTypeCode()
    {
        return static::ENTITY_CODE;
    }
    public function getValidColumnNames(): array
    {
        return $this->validColumnNames;
    }
    public function validateRow(array $rowData, $rowNum): bool
    {
        $id = (string)$rowData[self::ENTITY_ID_COLUMN];
        $storeId = (string)$rowData[self::STORE_ID];
        $name = (string)$rowData[self::NAME];
        $urlKey = (string)$rowData[self::URL_KEY];
        $parent = (string)$rowData[self::PARENT];
        $isActive = (string)$rowData[self::IS_ACTIVE];
        $includeInMenu = (string)$rowData[self::INCLUDE_IN_MENU];

        if ($id == '') {
            $this->addRowError('IdIsRequired', $rowNum);
        } elseif (!preg_match('/^[0-9]+$/', $id)) {
            $this->addRowError('IdIsNumber', $rowNum);
        }
        if ($storeId == '') {
            $this->addRowError('storeIdIsRequired', $rowNum);
        } elseif (!preg_match('/^[0-9]+$/', $storeId)) {
            $this->addRowError('StoreIdIsNumber', $rowNum);
        }
        if ($name == '') {
            $this->addRowError('nameIsRequired', $rowNum);
        }
        if ($urlKey == '') {
            $this->addRowError('urlKeyIsRequired', $rowNum);
        }
        if ($parent == '') {
            $this->addRowError('parentIsRequired', $rowNum);
        } elseif (!preg_match('/^[0-9]+$/', $parent)) {
            $this->addRowError('ParentIsNumber', $rowNum);
        }
        if ($isActive == '') {
            $this->addRowError('IsActiveIsRequired', $rowNum);
        } elseif ($isActive != "0" && $isActive != "1") {
            $this->addRowError('IsActiveFormat', $rowNum);
        }
        if ($includeInMenu == '')
        {
            $this->addRowError('IncludeInMenuIsRequired', $rowNum);
        }
        if (isset($this->_validatedRows[$rowNum])) {
            return !$this->getErrorAggregator()->isRowInvalid($rowNum);
        }

        $this->_validatedRows[$rowNum] = true;
        return !$this->getErrorAggregator()->isRowInvalid($rowNum);
    }
    private function initMessageTemplates()
    {
        $this->addMessageTemplate(
            'IdIsRequired',
            __('The entity_id cannot be empty.')
        );
        $this->addMessageTemplate(
            'IdIsNumber',
            __('The entity_id must be a number')
        );
        $this->addMessageTemplate(
            'storeIdIsRequired',
            __('The store_id cannot be empty.')
        );
        $this->addMessageTemplate(
            'StoreIdIsNumber',
            __('The store_id must be a number')
        );
        $this->addMessageTemplate(
            'nameIsRequired',
            __('The name cannot be empty.')
        );
        $this->addMessageTemplate(
            'urlKeyIsRequired',
            __('The url_key cannot be empty.')
        );
        $this->addMessageTemplate(
            'parentIsRequired',
            __('The parent cannot be empty.')
        );
        $this->addMessageTemplate(
            'ParentIsNumber',
            __('The parent must be a number')
        );
        $this->addMessageTemplate(
            'IsActiveIsRequired',
            __('The is_active cannot be empty.')
        );
        $this->addMessageTemplate(
            'IsActiveFormat',
            __('The is_active Wrong format [Format 0 or 1]')
        );
        $this->addMessageTemplate(
            'IncludeInMenuIsRequired',
            __('The include_in_menu cannot be empty.')
        );
    }
    protected function _importData(): bool
    {
        switch ($this->getBehavior()) {
            case Import::BEHAVIOR_DELETE:
                $this->deleteEntity();
                break;
            default :
                $this->saveAndReplaceEntity();
                break;
        }
        return true;
    }
    private function deleteEntity(): bool
    {
        $rows = [];
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            foreach ($bunch as $rowNum => $rowData) {
                $this->validateRow($rowData, $rowNum);
                if (!$this->getErrorAggregator()->isRowInvalid($rowNum)) {
                    $rowId = $rowData[static::ENTITY_ID_COLUMN];
                    $rows[] = $rowId;
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);
                }
            }
        }

        if ($rows) {
            return $this->deleteEntityFinish(array_unique($rows));
        }
        return false;
    }
    private function saveAndReplaceEntity()
    {
        $behavior = $this->getBehavior();
        $rows = [];
        while ($bunch = $this->_dataSourceModel->getNextBunch()) {
            $entityList = [];

            foreach ($bunch as $rowNum => $row) {
                if (!$this->validateRow($row, $rowNum)) {
                    continue;
                }

                if ($this->getErrorAggregator()->hasToBeTerminated()) {
                    $this->getErrorAggregator()->addRowToSkip($rowNum);

                    continue;
                }

                $rowId = $row[static::ENTITY_ID_COLUMN];
                $rows[] = $rowId;
                $columnValues = [];

                foreach ($this->getAvailableColumns() as $columnKey) {
                    $columnValues[$columnKey] = $row[$columnKey];
                }

                $entityList[$rowId][] = $columnValues;
            }

            if (Import::BEHAVIOR_REPLACE === $behavior) {
                if ($rows && $this->deleteEntityFinish(array_unique($rows))) {
                    $this->saveEntityFinish($entityList);
                }
            } elseif (Import::BEHAVIOR_APPEND === $behavior) {
                $this->saveEntityFinish($entityList);
            }
        }
    }
    private function saveEntityFinish(array $entityData): bool
    {
        if ($entityData) {
            $rows = [];
            foreach ($entityData as $entityRows) {
                foreach ($entityRows as $row) {
                    $rows[] = $row;
                }
            }
            if ($rows) {
                $this->addOrUpdateEntity($rows);
                return true;
            }
            return false;
        }
    }
    private function addOrUpdateEntity($rows)
    {
        $countCreate = 0;
        $countUpdate = 0;
        foreach ($rows as $row) {
            $modelCategory = $this->modelCategoryFactory->create();
            if ($modelCategory->load($row['entity_id'])->getId() == null) {
                $countCreate++;
            } else {
                $modelCategory->setEntityId($row['entity_id']);
                $countUpdate++;
            }
            $modelCategory->setStoreId($row['store_id']);
            $modelCategory->setParentId($row['parent']);
            $modelCategory->setIsActive($row['is_active']);
            $modelCategory->setIncludeInMenu($row['include_in_menu']);
            $modelCategory->setName($row['name']);
            $modelCategory->setAvailableSortBy($row['available_sort_by']);
            $modelCategory->setUrlKey($row['url_key']);
            $modelCategory->setPosition('position');
            $this->categoryRepository->save($modelCategory);
        }
        $this->countItemsCreated = (int)$countCreate;
        $this->countItemsUpdated = (int)$countUpdate;
    }
    private function deleteEntityFinish(array $entityIds): bool
    {
        if ($entityIds) {
            try {
                $this->countItemsDeleted += $this->connection->delete(
                    $this->connection->getTableName(static::TABLE),
                    $this->connection->quoteInto(static::ENTITY_ID_COLUMN . ' IN (?)', $entityIds)
                );
                return true;
            } catch (Exception $e) {
                return false;
            }
        }
        return false;
    }
    private function getAvailableColumns(): array
    {
        return $this->validColumnNames;
    }
}